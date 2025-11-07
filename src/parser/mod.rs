use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::executor::error::ExecutorError;

pub fn parse(query: &str) -> Result<Vec<Statement>, ExecutorError> {
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, query)
        .map_err(|e| ExecutorError::Parse(format!("Parse error: {}", e)))
}