use pgwire::error::{ErrorInfo, PgWireError};

pub enum ExecutorError {
    Parse(String),
    Plan(String),
    Execution(String),
    UnsupportedStatement(String),
    // StorageError(storage::Error)
}

impl From<ExecutorError> for PgWireError {
    fn from(e: ExecutorError) -> PgWireError {
        match e {
            ExecutorError::Parse(msg) => PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42601".to_string(), // syntax_error
                msg,
            ))),
            ExecutorError::UnsupportedStatement(msg) => PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "0A000".to_string(), // feature_not_supported
                msg,
            ))),
            ExecutorError::Plan(msg) => PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42P01".to_string(), // undefined_table
                msg,
            ))),
            ExecutorError::Execution(msg) => PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(), // internal_error
                msg,
            )))
        }
    }
}

