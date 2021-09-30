use async_trait::async_trait;
use tokio_postgres::{ Client, NoTls, Row, RowStream };
use tokio_postgres::types::{ BorrowToSql };
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::{ Tokenizer, Token, Word };
use sqlparser::dialect::keywords::Keyword;
use std::pin::Pin;
use futures_util::StreamExt;
use std::sync::Arc;
use std::collections::HashMap;

pub struct Driver;

#[async_trait]
impl rdbc_async::sql::Driver for Driver{
    async fn connect(&self, connection_uri: &str) -> rdbc_async::sql::Result<Box<dyn rdbc_async::sql::Connection>> {
        let (c, connection) = tokio_postgres::connect(connection_uri,NoTls).await.map_err(to_rdbc_error)?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        Ok(Box::new(Connection::new(c)))
    }
}

struct Connection {
    conn: Client,
}
impl Connection {
    pub fn new(conn: Client) -> Self {
        Self { conn }
    }
}

#[async_trait]
impl rdbc_async::sql::Connection for Connection {
    async fn create(&self, sql: &str) -> rdbc_async::sql::Result<Box<dyn rdbc_async::sql::Statement + '_>> {
        self.prepare(sql).await
    }

    async fn prepare(&self, sql: &str) -> rdbc_async::sql::Result<Box<dyn rdbc_async::sql::Statement + '_>> {
        // translate SQL, mapping ? into $1 style bound param placeholder
        let dialect = PostgreSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut i = 0;
        let tokens: Vec<Token> = tokens
            .iter()
            .map(|t| match t {
                Token::Char(c) if *c == '?' => {
                    i += 1;
                    Token::Word(Word {
                        value: format!("${}", i),
                        quote_style: None,
                        keyword: Keyword::NONE,
                    })
                }
                _ => t.clone(),
            })
            .collect();
        let sql = tokens
            .iter()
            .map(|t| format!("{}", t))
            .collect::<Vec<String>>()
            .join("");

        Ok(Box::new(Statement {
            conn: &self.conn,
            sql,
        }))
    }
}

struct Statement<'a> {
    conn: &'a Client,
    sql: String,
}

#[async_trait]
impl<'a> rdbc_async::sql::Statement for Statement<'a> {
    async fn execute_query(
        &self,
        params: &[rdbc_async::sql::Value],
    ) -> rdbc_async::sql::Result<Box<dyn rdbc_async::sql::ResultSet + '_>> {

        let rows = self
            .conn
            .query_raw(self.sql.as_str(), params.iter().map(|d|{
                match d {
                    rdbc_async::sql::Value::String(s)=>s.borrow_to_sql(),
                    rdbc_async::sql::Value::Int32(i)=>i.borrow_to_sql(),
                    rdbc_async::sql::Value::UInt32(n)=>n.borrow_to_sql(),
                    rdbc_async::sql::Value::Int64(i)=>i.borrow_to_sql(),
                    // rdbc_async::sql::Value::UInt64(n)=>n.borrow_to_sql(),
                    rdbc_async::sql::Value::Float32(n)=>n.borrow_to_sql(),
                    rdbc_async::sql::Value::Float64(n)=>n.borrow_to_sql()
                }
            })).await
            .map_err(to_rdbc_error)?;
        Ok(Box::new(ResultSet { fetched_rows:HashMap::new(),current_row_index:0,current_row:Option::None,rows:Box::pin(rows) }))
    }

    async fn execute_update(&self, params: &[rdbc_async::sql::Value]) -> rdbc_async::sql::Result<u64> {
        self.conn
            .execute_raw(self.sql.as_str(), params.iter().map(|d|{
                match d {
                    rdbc_async::sql::Value::String(s)=>s.borrow_to_sql(),
                    rdbc_async::sql::Value::Int32(i)=>i.borrow_to_sql(),
                    rdbc_async::sql::Value::UInt32(n)=>n.borrow_to_sql(),
                    rdbc_async::sql::Value::Int64(i)=>i.borrow_to_sql(),
                    // rdbc_async::sql::Value::UInt64(n)=>n.borrow_to_sql(),
                    rdbc_async::sql::Value::Float32(n)=>n.borrow_to_sql(),
                    rdbc_async::sql::Value::Float64(n)=>n.borrow_to_sql()
                }
            })).await
            .map_err(to_rdbc_error)
    }
}

struct ResultSet {
    current_row_index:u64,
    fetched_rows:HashMap<u64,Arc<Row>>,
    current_row : Option<Arc<Row>>,
    rows: Pin<Box<RowStream>>,
}

macro_rules! impl_resultset_fns {
    ($($fn: ident -> $ty: ty),*) => {
        $(
            fn $fn(&self, i: u64) -> rdbc_async::sql::Result<$ty> {
                if let Some(row)=&self.current_row{
                    Ok(row.get(i as usize))
                } else {
                    Err(rdbc_async::sql::Error::General("Something went wrong".to_owned()))
                }
            }
        )*
    }
}

#[async_trait]
impl rdbc_async::sql::ResultSet for ResultSet {
    async fn next(&mut self) -> bool {
        if let Some((arc,new_index,fetched)) = if self.fetched_rows.len() as u64 > self.current_row_index + 1 {
            self.fetched_rows.get(&(self.current_row_index + 1)).map(|arc| (arc.clone(),self.current_row_index +1,false))
        } else {
            if let Some(Ok(row))=self.rows.next().await {
                let arc = Arc::new(row);
                let new_index = if self.current_row_index == 0 && self.fetched_rows.len() == 0 {
                    0
                } else {
                    self.current_row_index+1
                };
                Option::Some((arc,new_index,true))
            } else {
                Option::None
            }
        } {
            if fetched {
                self.fetched_rows.insert(new_index, arc.clone());
            }
            self.current_row_index = new_index;
            self.current_row = Option::Some(arc);
            true
        } else {
            false
        }
    }

    async fn previous(&mut self) -> bool {
        if let Some(row)=self.fetched_rows.get(&(self.current_row_index-1)) {
            self.current_row = Option::Some(row.clone());
            true
        } else {
            false
        }
    }

    async fn first(&mut self) -> bool {
        if self.current_row_index == 0 && self.fetched_rows.len() == 0 {
            self.next().await
        } else {
            if let Some(row)=self.fetched_rows.get(&(0)) {
                self.current_row = Option::Some(row.clone());
                self.current_row_index = 0;
                true
            } else {
                false
            }
        }
    }

    async fn last(&mut self) -> bool {
        while self.next().await {

        };
        if self.fetched_rows.len() > 0 {
            return true
        } {
            return false
        }
    }

    async fn absolute(&mut self,row:u64) -> bool {
        if self.fetched_rows.len() as u64 <= row {
            self.current_row_index = self.fetched_rows.len() as u64 -1;
            while self.next().await {
                if self.current_row_index == row {
                    break;
                }
            }
        }

        if let Some(_) = self.fetched_rows.get(&row) {
            true
        } else {
            false
        }

    }

    impl_resultset_fns! {
        get_i8 -> i8,
        get_i16 -> i16,
        get_i32 -> i32,
        get_i64 -> i64,
        get_f32 -> f32,
        get_f64 -> f64,
        get_string -> String,
        get_bytes -> Vec<u8>
    }
}

/// Convert a Postgres error into an rsdb error
fn to_rdbc_error(e: tokio_postgres::error::Error) -> rdbc_async::sql::Error {
    rdbc_async::sql::Error::General(format!("{:?}", e))
}