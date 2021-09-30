use async_trait::async_trait;

#[derive(Debug)]
pub enum Error {
    General(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub enum Value {
    Int32(i32),
    Float32(f32),
    Int64(i64),
    Float64(f64),
    UInt32(u32),
    // UInt64(u64),
    String(String),
    //TODO add other types
}

#[async_trait]
pub trait Driver: Sync + Send {
    /// Create a connection to the database. Note that connections are intended to be used
    async fn connect(&self,connection_uri: &str) -> Result<Box<dyn Connection>>;
}

#[async_trait]
pub trait Connection {
    /// Create a statement for execution
    async fn create(&self, sql: &str) -> Result<Box<dyn Statement + '_>>;

    /// Create a prepared statement for execution
    async fn prepare(&self, sql: &str) -> Result<Box<dyn Statement + '_>>;
}

/// Represents an executable statement
#[async_trait]
pub trait Statement {
    /// Execute a query that is expected to return a result set, such as a `SELECT` statement
    async fn execute_query(&self, params: &[Value]) -> Result<Box<dyn ResultSet + '_>>;

    /// Execute a query that is expected to update some rows.
    async fn execute_update(&self, params: &[Value]) -> Result<u64>;
}

/// Result set from executing a query against a statement
#[async_trait]
pub trait ResultSet {

    /// Move the cursor to the next available row if one exists and return true if it does
    async fn next(&mut self) -> bool;

    /// Move the cursor to the next available row if one exists and return true if it does
    async fn previous(&mut self) -> bool;
    //
    /// Move the cursor to the next available row if one exists and return true if it does
    async fn first(&mut self) -> bool;
    //
    /// Move the cursor to the next available row if one exists and return true if it does
    async fn last(&mut self) -> bool;
    //
    /// Move the cursor to the next available row if one exists and return true if it does
    async fn absolute(&mut self,row:u64) -> bool;
    //
    // /// Move the cursor to the next available row if one exists and return true if it does
    // async fn relative(&mut self) -> bool;

    fn get_i8(&self, i: u64) -> Result<i8>;
    fn get_i16(&self, i: u64) -> Result<i16>;
    fn get_i32(&self, i: u64) -> Result<i32>;
    fn get_i64(&self, i: u64) -> Result<i64>;
    fn get_f32(&self, i: u64) -> Result<f32>;
    fn get_f64(&self, i: u64) -> Result<f64>;
    fn get_string(&self, i: u64) -> Result<String>;
    fn get_bytes(&self, i: u64) -> Result<Vec<u8>>;
}