use std::path::Path;
use std::sync::Arc;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub use error::{Error, Result};
pub use params::{Params, Value};
pub use rows::{Row, Rows};
use crate::db::DatabaseInner;

mod base;
mod error;
mod params;
mod rows;
mod db;

/// Database handle - owns the file and shared state.
/// Cheap to clone (uses Arc internally).
#[derive(Debug)]
pub struct Database {
    inner: Arc<DatabaseInner>,
}

impl Database {
    /// Opens a database at the given path.
    /// Creates a new database if it doesn't exist.
    pub fn open(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Database {
            inner: Arc::from(DatabaseInner::new()),
        })
    }

    /// Creates a connection handle.
    /// Connections are cheap and can be created freely.
    pub fn connect(&self) -> Connection<'_> {
        Connection {
            db: self,
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        todo!("close file handlers, flush buffers, release locsk")
    }
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Database {
            inner: self.inner.clone(),
        }
    }
}

/// Connection handle - borrows the Database.
/// Can execute queries and begin transactions.
pub struct Connection<'db> {
    db: &'db Database,
}

impl<'db> Connection<'db> {
    /// Executes a statement and returns the number of affected rows.
    pub fn execute(&self, sql: &str, params: impl Params) -> Result<u64> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| Error::Parse(e.to_string()))?;

        if ast.is_empty() {
            return Ok(0);
        }

        let param_values = params.into_vec();
        self.db.inner.execute_statement(&ast[0], &param_values)
    }

    /// Executes a query and returns rows.
    pub fn query(&self, sql: &str, params: impl Params) -> Result<Rows> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| Error::Parse(e.to_string()))?;

        if ast.is_empty() {
            return Ok(Rows::new(vec![]));
        }

        let param_values = params.into_vec();
        self.db.inner.query_statement(&ast[0], &param_values)
    }

    /// Begins a transaction. (TODO: implement properly with MVCC)
    pub fn begin(&mut self) -> Result<()> {
        // For now, transactions are no-ops in the in-memory implementation
        Ok(())
    }
}

#[cfg(feature = "sqlx")]
mod sqlx_adapter {
    use crate::Database;

    impl sqlx::Database for Database {
        type Connection = ();
        type TransactionManager = ();
        type Row = ();
        type QueryResult = ();
        type Column = ();
        type TypeInfo = ();
        type Value = ();
        type ValueRef<'r> = ();
        type Arguments<'q> = ();
        type ArgumentBuffer<'q> = ();
        type Statement<'q> = ();
        const NAME: &'static str = "";
        const URL_SCHEMES: &'static [&'static str] = &[];
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_insert_select() {
        let db = Database::open("test.db").unwrap();
        let conn = db.connect();

        // CREATE TABLE
        conn.execute("CREATE TABLE users (id INT, name TEXT, age INT)", params![]).unwrap();

        // INSERT (currently no param binding in SQL, but demonstrates API)
        conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)", params![]).unwrap();
        conn.execute("INSERT INTO users VALUES (2, 'Bob', 25)", params![]).unwrap();

        // SELECT
        let mut rows = conn.query("SELECT * FROM users", params![]).unwrap();

        let row1 = rows.next().unwrap();
        assert_eq!(row1.len(), 3);

        let row2 = rows.next().unwrap();
        assert_eq!(row2.len(), 3);

        assert!(rows.next().is_none());
    }

    #[test]
    fn test_parameterized_insert() {
        let db = Database::open("test2.db").unwrap();
        let conn = db.connect();

        conn.execute("CREATE TABLE products (id INT, name TEXT, price REAL)", params![]).unwrap();

        // Use $1, $2, $3 style placeholders
        conn.execute(
            "INSERT INTO products VALUES ($1, $2, $3)",
            params![1, "Widget", 9.99]
        ).unwrap();

        conn.execute(
            "INSERT INTO products VALUES ($1, $2, $3)",
            params![2, "Gadget", 19.99]
        ).unwrap();

        // Query back
        let mut rows = conn.query("SELECT * FROM products", params![]).unwrap();

        let row1 = rows.next().unwrap();
        assert_eq!(row1.len(), 3);
        assert_eq!(row1.get(0), Some(&Value::Integer(1)));
        assert_eq!(row1.get(1), Some(&Value::Text("Widget".to_string())));

        let row2 = rows.next().unwrap();
        assert_eq!(row2.get(0), Some(&Value::Integer(2)));
        assert_eq!(row2.get(1), Some(&Value::Text("Gadget".to_string())));
    }
}
