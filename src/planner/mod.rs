use sqlparser::ast::{Statement, CreateTable, Insert};
use tracing::debug;

use crate::executor::error::ExecutorError;
use crate::types::{Schema, Column, DataType};

#[derive(Debug)]
pub enum Operator {
    /// Scan all rows from a table
    TableScan {
        table: String,
    },
    /// Filter rows with a predicate
    Filter {
        input: Box<Operator>,
        predicate: sqlparser::ast::Expr,
    },
    /// Project columns from input
    Project {
        input: Box<Operator>,
        columns: Vec<sqlparser::ast::Expr>,
    },
    /// Aggregate with GROUP BY
    Aggregate {
        input: Box<Operator>,
        group_by: Vec<sqlparser::ast::Expr>,
        aggregates: Vec<sqlparser::ast::Expr>,
    },
    /// Limit/offset rows
    Limit {
        input: Box<Operator>,
        limit: u64,
        offset: Option<u64>,
    },
}

pub fn plan(stmt: &Statement) -> Result<Operator, ExecutorError> {
    debug!("planning statement");

    match stmt {
        Statement::Query(query) => plan_select(query),
        Statement::StartTransaction { .. } => {
            debug!("plan: start transaction (handled by executor)");
            Err(ExecutorError::UnsupportedStatement(
                "Transactions handled at executor level".to_string(),
            ))
        }
        Statement::Rollback { .. } => {
            debug!("plan: rollback (handled by executor)");
            Err(ExecutorError::UnsupportedStatement(
                "Transactions handled at executor level".to_string(),
            ))
        }
        Statement::Commit { .. } => {
            debug!("plan: commit (handled by executor)");
            Err(ExecutorError::UnsupportedStatement(
                "Transactions handled at executor level".to_string(),
            ))
        }
        _ => {
            debug!("plan: unsupported statement");
            Err(ExecutorError::UnsupportedStatement(format!(
                "Unsupported statement: {:?}",
                stmt
            )))
        }
    }
}

fn plan_select(query: &sqlparser::ast::Query) -> Result<Operator, ExecutorError> {
    if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
        // Start with TableScan if there's a FROM clause
        let mut plan = if select.from.is_empty() {
            // No FROM = constant expression (e.g., SELECT 1)
            debug!("plan: constant select (no FROM)");
            Operator::TableScan {
                table: "__constant__".to_string(),
            }
        } else if select.from.len() == 1 {
            let table_name = extract_table_name(&select.from[0])?;
            debug!(table = %table_name, "plan: table scan");
            Operator::TableScan { table: table_name }
        } else {
            return Err(ExecutorError::UnsupportedStatement(
                "Multiple tables not yet supported".to_string(),
            ));
        };

        // Add WHERE filter if present
        if let Some(selection) = &select.selection {
            debug!("plan: adding filter");
            plan = Operator::Filter {
                input: Box::new(plan),
                predicate: selection.clone(),
            };
        }

        // Add projection (SELECT columns)
        if !select.projection.is_empty() {
            let columns = select
                .projection
                .iter()
                .map(|item| match item {
                    sqlparser::ast::SelectItem::UnnamedExpr(expr) => expr.clone(),
                    sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => expr.clone(),
                    sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => {
                        // Placeholder for wildcard - will expand columns during execution
                        sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("*"))
                    }
                    sqlparser::ast::SelectItem::Wildcard(_) => {
                        sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("*"))
                    }
                })
                .collect::<Vec<_>>();
            debug!(column_count = columns.len(), "plan: adding projection");
            plan = Operator::Project {
                input: Box::new(plan),
                columns,
            };
        }

        // Add LIMIT if present
        if let Some(limit_clause) = &query.limit_clause {
            if let sqlparser::ast::LimitClause::LimitOffset { limit: Some(limit_expr), offset, .. } = limit_clause {
                // Extract limit value from expression
                if let sqlparser::ast::Expr::Value(val) = limit_expr {
                    if let sqlparser::ast::Value::Number(num_str, _) = &val.value {
                        if let Ok(limit_val) = num_str.parse::<u64>() {
                            let offset_val = if let Some(off) = offset {
                                match off {
                                    sqlparser::ast::Offset { value: sqlparser::ast::Expr::Value(v), .. } => {
                                        if let sqlparser::ast::Value::Number(off_str, _) = &v.value {
                                            off_str.parse::<u64>().ok()
                                        } else {
                                            None
                                        }
                                    }
                                    _ => None
                                }
                            } else {
                                None
                            };

                            debug!(limit = limit_val, offset = ?offset_val, "plan: adding limit");
                            plan = Operator::Limit {
                                input: Box::new(plan),
                                limit: limit_val,
                                offset: offset_val,
                            };
                        }
                    }
                }
            }
        }

        Ok(plan)
    } else {
        Err(ExecutorError::UnsupportedStatement(
            "Only SELECT queries supported".to_string(),
        ))
    }
}

fn extract_table_name(table_with_joins: &sqlparser::ast::TableWithJoins) -> Result<String, ExecutorError> {
    match &table_with_joins.relation {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            Ok(name.0.iter()
                .filter_map(|part| part.as_ident())
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join("."))
        }
        _ => Err(ExecutorError::UnsupportedStatement(
            "Only simple table scans supported".to_string(),
        )),
    }
}

pub fn extract_create_table(stmt: &CreateTable) -> Result<(String, Schema), ExecutorError> {
    debug!("extracting create table");

    // Extract table name
    let table_name = stmt.name.0.iter()
        .filter_map(|part| part.as_ident())
        .map(|ident| ident.value.clone())
        .collect::<Vec<_>>()
        .join(".");

    if table_name.is_empty() {
        return Err(ExecutorError::Execution("Table name is empty".to_string()));
    }

    debug!(table = %table_name, "extracting columns");

    // Extract columns
    let mut columns = Vec::new();
    for col_def in &stmt.columns {
        let col_name = col_def.name.value.clone();
        let data_type = sql_type_to_data_type(&col_def.data_type)?;

        columns.push(Column {
            name: col_name,
            data_type,
        });
    }

    if columns.is_empty() {
        return Err(ExecutorError::Execution(
            "CREATE TABLE requires at least one column".to_string(),
        ));
    }

    Ok((table_name, Schema::new(columns)))
}

pub fn extract_insert(stmt: &Insert) -> Result<(String, Vec<Vec<sqlparser::ast::Expr>>), ExecutorError> {
    debug!("extracting insert statement");

    // Extract table name from TableObject
    let table_name = match &stmt.table {
        sqlparser::ast::TableObject::TableName(name) => {
            name.0.iter()
                .filter_map(|part| part.as_ident())
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join(".")
        }
        sqlparser::ast::TableObject::TableFunction(_) => {
            return Err(ExecutorError::UnsupportedStatement(
                "Table functions in INSERT not supported".to_string(),
            ));
        }
    };

    if table_name.is_empty() {
        return Err(ExecutorError::Execution("Table name is empty".to_string()));
    }

    debug!(table = %table_name, "extracting insert rows");

    // Extract rows from INSERT ... VALUES (...)
    let mut rows = Vec::new();

    if let Some(source) = &stmt.source {
        // The source is a Query, extract VALUES from it
        if let sqlparser::ast::SetExpr::Values(values) = &*source.body {
            for row in &values.rows {
                rows.push(row.clone());
            }
        } else {
            return Err(ExecutorError::Execution(
                "INSERT with SELECT not yet supported".to_string(),
            ));
        }
    } else {
        return Err(ExecutorError::Execution(
            "INSERT without VALUES not yet supported".to_string(),
        ));
    }

    if rows.is_empty() {
        return Err(ExecutorError::Execution("INSERT requires at least one row".to_string()));
    }

    Ok((table_name, rows))
}

fn sql_type_to_data_type(data_type: &sqlparser::ast::DataType) -> Result<DataType, ExecutorError> {
    use sqlparser::ast::DataType as SqlDataType;

    match data_type {
        SqlDataType::Int(_)
        | SqlDataType::BigInt(_)
        | SqlDataType::SmallInt(_)
        | SqlDataType::Integer(_) => Ok(DataType::Int),

        SqlDataType::Float(_)
        | SqlDataType::Real
        | SqlDataType::Double(_)
        | SqlDataType::Numeric(_)
        | SqlDataType::Decimal(_) => Ok(DataType::Float),

        SqlDataType::Varchar(_)
        | SqlDataType::Char(_)
        | SqlDataType::Text
        | SqlDataType::String(_) => Ok(DataType::String),

        SqlDataType::Boolean => Ok(DataType::Bool),
        _ => {
            debug!(data_type = ?data_type, "unsupported data type");
            Err(ExecutorError::UnsupportedStatement(format!(
                "Unsupported data type: {:?}",
                data_type
            )))
        }
    }
}