use sqlparser::ast::Statement;
use crate::executor::error::ExecutorError;

#[derive(Debug)]
pub enum Plan {
    StartTransaction,
    Rollback,
    Commit,
    SelectOne,
    Unsupported(String),
}

pub fn plan(stmt: &Statement) -> Result<Plan, ExecutorError> {
    match stmt {
        Statement::StartTransaction { .. } => Ok(Plan::StartTransaction),
        Statement::Rollback { .. } => Ok(Plan::Rollback),
        Statement::Commit { .. } => Ok(Plan::Commit),
        Statement::Query(query) => {
            // Check if it's "SELECT 1"
            if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
                if select.projection.len() == 1 {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                        if let sqlparser::ast::Expr::Value(val) = expr {
                            if let sqlparser::ast::Value::Number(n, _) = &val.value {
                                if n == "1" && select.from.is_empty() {
                                    return Ok(Plan::SelectOne);
                                }
                            }
                        }
                    }
                }
            }
            Ok(Plan::Unsupported("Only SELECT 1 is supported".to_string()))
        }
        _ => Ok(Plan::Unsupported(format!("Unsupported statement: {:?}", stmt))),
    }
}