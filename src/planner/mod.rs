use sqlparser::ast::{Statement, CreateTable, Insert, CreateIndex};
use tracing::debug;

use crate::executor::error::ExecutorError;
use crate::types::{Schema, Column, DataType};

#[derive(Debug)]
pub enum Operator {
    /// Scan all rows from a table
    TableScan {
        table: String,
    },
    /// Index scan for exact key lookup
    IndexScan {
        table: String,
        column: String,
        value: sqlparser::ast::Expr,
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
        let (mut plan, table_name_opt) = if select.from.is_empty() {
            // No FROM = constant expression (e.g., SELECT 1)
            debug!("plan: constant select (no FROM)");
            (Operator::TableScan {
                table: "__constant__".to_string(),
            }, None)
        } else if select.from.len() == 1 {
            let table_name = extract_table_name(&select.from[0])?;
            debug!(table = %table_name, "plan: table scan");
            (Operator::TableScan { table: table_name.clone() }, Some(table_name))
        } else {
            return Err(ExecutorError::UnsupportedStatement(
                "Multiple tables not yet supported".to_string(),
            ));
        };

        // Try to use IndexScan for equality predicates on primary key
        if let Some(selection) = &select.selection {
            if let Some(table_name) = &table_name_opt {
                // Check if selection is a simple equality (col = value)
                if let Some((col_name, value_expr)) = try_extract_equality(selection) {
                    debug!(column = %col_name, "plan: attempting index scan");
                    plan = Operator::IndexScan {
                        table: table_name.clone(),
                        column: col_name,
                        value: value_expr,
                    };
                } else {
                    debug!("plan: adding filter (not index-able)");
                    plan = Operator::Filter {
                        input: Box::new(plan),
                        predicate: selection.clone(),
                    };
                }
            } else {
                debug!("plan: adding filter");
                plan = Operator::Filter {
                    input: Box::new(plan),
                    predicate: selection.clone(),
                };
            }
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

pub fn extract_create_table(stmt: &CreateTable) -> Result<(String, Schema, String), ExecutorError> {
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
            is_primary_key: false,
        });
    }

    if columns.is_empty() {
        return Err(ExecutorError::Execution(
            "CREATE TABLE requires at least one column".to_string(),
        ));
    }

    // Extract PRIMARY KEY constraint
    let mut primary_key_col = None;
    for constraint in &stmt.constraints {
        use sqlparser::ast::TableConstraint;
        if let TableConstraint::PrimaryKey { columns: pk_cols, .. } = constraint {
            if pk_cols.is_empty() {
                return Err(ExecutorError::Execution(
                    "PRIMARY KEY constraint requires at least one column".to_string(),
                ));
            }
            if pk_cols.len() > 1 {
                return Err(ExecutorError::UnsupportedStatement(
                    "Composite primary keys not yet supported".to_string(),
                ));
            }

            // Extract column name from first PK column (IndexColumn)
            let pk_col_name = match &pk_cols[0].column.expr {
                sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
                _ => return Err(ExecutorError::Execution(
                    "PRIMARY KEY column must be an identifier".to_string(),
                )),
            };

            // Mark the column as primary key
            if let Some(col) = columns.iter_mut().find(|c| c.name == pk_col_name) {
                col.is_primary_key = true;
                primary_key_col = Some(pk_col_name);
            } else {
                return Err(ExecutorError::Execution(
                    format!("PRIMARY KEY column '{}' not found in table definition", pk_col_name),
                ));
            }
        }
    }

    let primary_key_col = primary_key_col.ok_or_else(|| {
        ExecutorError::Execution(
            "CREATE TABLE requires a PRIMARY KEY constraint (like Postgres)".to_string(),
        )
    })?;

    debug!(table = %table_name, primary_key = %primary_key_col, "extracted create table");

    Ok((table_name, Schema::new(columns), primary_key_col))
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

pub fn extract_create_index(stmt: &CreateIndex) -> Result<(String, String, String), ExecutorError> {
    debug!("extracting create index");

    // Extract index name (required)
    let index_name = match &stmt.name {
        Some(name) => {
            name.0.iter()
                .filter_map(|part| part.as_ident())
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join(".")
        }
        None => return Err(ExecutorError::Execution("CREATE INDEX requires an index name".to_string())),
    };

    if index_name.is_empty() {
        return Err(ExecutorError::Execution("Index name is empty".to_string()));
    }

    // Extract table name
    let table_name = stmt.table_name.0.iter()
        .filter_map(|part| part.as_ident())
        .map(|ident| ident.value.clone())
        .collect::<Vec<_>>()
        .join(".");

    if table_name.is_empty() {
        return Err(ExecutorError::Execution("Table name is empty".to_string()));
    }

    debug!(index = %index_name, table = %table_name, "extracting index columns");

    // Extract column name (only support single column for now)
    if stmt.columns.is_empty() {
        return Err(ExecutorError::Execution(
            "CREATE INDEX requires at least one column".to_string(),
        ));
    }

    if stmt.columns.len() > 1 {
        return Err(ExecutorError::UnsupportedStatement(
            "Multi-column indexes not yet supported".to_string(),
        ));
    }

    // IndexColumn has a `column` field which is an OrderByExpr
    let column_name = match &stmt.columns[0].column.expr {
        sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
        _ => return Err(ExecutorError::Execution(
            "Index column must be an identifier".to_string(),
        )),
    };

    // Extract index type from USING clause (defaults to "btree")
    let index_type = if let Some(using) = &stmt.using {
        // using is an IndexType enum
        match using {
            sqlparser::ast::IndexType::BTree => "btree".to_string(),
            sqlparser::ast::IndexType::Hash => "hash".to_string(),
            sqlparser::ast::IndexType::GIN => "gin".to_string(),
            sqlparser::ast::IndexType::GiST => "gist".to_string(),
            sqlparser::ast::IndexType::SPGiST => "spgist".to_string(),
            sqlparser::ast::IndexType::BRIN => "brin".to_string(),
            sqlparser::ast::IndexType::Bloom => "bloom".to_string(),
            sqlparser::ast::IndexType::Custom(ident) => ident.value.to_lowercase(),
        }
    } else {
        "btree".to_string()
    };

    debug!(index = %index_name, table = %table_name, column = %column_name, index_type = %index_type, "extracted create index");

    Ok((table_name, column_name, index_type))
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

/// Try to extract a simple equality predicate (col = value) from a WHERE clause
/// Returns Some((column_name, value_expr)) if matched, None otherwise
fn try_extract_equality(expr: &sqlparser::ast::Expr) -> Option<(String, sqlparser::ast::Expr)> {
    use sqlparser::ast::{BinaryOperator, Expr};

    match expr {
        // Match: col = value
        Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
            // Try left=Identifier, right=Value
            if let Expr::Identifier(ident) = &**left {
                return Some((ident.value.clone(), (**right).clone()));
            }
            // Try right=Identifier, left=Value (value = col)
            if let Expr::Identifier(ident) = &**right {
                return Some((ident.value.clone(), (**left).clone()));
            }
            None
        }
        _ => None,
    }
}