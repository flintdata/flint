use std::collections::HashMap;
use std::sync::RwLock;
use sqlparser::ast::{Expr, Statement};
use crate::{Error, Rows, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Integer,
    Real,
    Text,
    Blob,
}

#[derive(Debug)]
struct Table {
    rows: Vec<Vec<Value>>,
    cols: Vec<(String, ColumnType)>,
}

#[derive(Debug)]
pub(crate) struct DatabaseInner {
    tables: RwLock<HashMap<String, Table>>,
}

impl DatabaseInner {
    pub(crate) fn new() -> Self {
        DatabaseInner {
            tables: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn execute_statement(&self, stmt: &Statement, params: &[Value]) -> crate::Result<u64> {
        match stmt {
            Statement::CreateTable(create) => {
                let table_name = create.name.to_string();
                let mut columns = Vec::new();

                for col in &create.columns {
                    let col_name = col.name.to_string();
                    let col_type = match &col.data_type {
                        sqlparser::ast::DataType::Int(_)
                        | sqlparser::ast::DataType::Integer(_)
                        | sqlparser::ast::DataType::BigInt(_) => ColumnType::Integer,
                        sqlparser::ast::DataType::Real
                        | sqlparser::ast::DataType::Float(_)
                        | sqlparser::ast::DataType::Double(_) => ColumnType::Real,
                        sqlparser::ast::DataType::Text
                        | sqlparser::ast::DataType::Varchar(_)
                        | sqlparser::ast::DataType::Char(_) => ColumnType::Text,
                        sqlparser::ast::DataType::Blob(_) => ColumnType::Blob,
                        _ => ColumnType::Text,
                    };
                    columns.push((col_name, col_type));
                }

                let mut tables = self.tables.write().unwrap();
                tables.insert(table_name, Table {
                    cols: columns,
                    rows: Vec::new(),
                });

                Ok(0)
            }
            Statement::Insert(insert) => {
                let table_name = insert.table.to_string();

                let mut tables = self.tables.write().unwrap();
                let table = tables.get_mut(&table_name)
                    .ok_or_else(|| Error::InvalidOperation(format!("Table {} not found", table_name)))?;

                if let Some(values_list) = &insert.source {
                    if let sqlparser::ast::SetExpr::Values(values) = &*values_list.body {
                        for row_values in &values.rows {
                            let mut row = Vec::new();
                            for expr in row_values {
                                let val = Self::eval_expr(expr, params)?;
                                row.push(val);
                            }
                            table.rows.push(row);
                        }
                        return Ok(values.rows.len() as u64);
                    }
                }

                Ok(0)
            }
            _ => Err(Error::NotImplemented),
        }
    }

    pub(crate) fn query_statement(&self, stmt: &Statement, _params: &[Value]) -> crate::Result<Rows> {
        match stmt {
            Statement::Query(query) => {
                if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
                    if let Some(sqlparser::ast::TableWithJoins { relation, .. }) = select.from.first() {
                        if let sqlparser::ast::TableFactor::Table { name, .. } = &relation {
                            let table_name = name.to_string();
                            let tables = self.tables.read().unwrap();
                            let table = tables.get(&table_name)
                                .ok_or_else(|| Error::InvalidOperation(format!("Table {} not found", table_name)))?;

                            let rows = table.rows.clone();
                            return Ok(Rows::new(rows));
                        }
                    }
                }
                Err(Error::NotImplemented)
            }
            _ => Err(Error::InvalidOperation("Not a query".to_string())),
        }
    }

    fn eval_expr(expr: &Expr, params: &[Value]) -> crate::Result<Value> {
        match expr {
            Expr::Value(val_span) => {
                match &val_span.value {
                    sqlparser::ast::Value::Number(n, _) => {
                        if let Ok(i) = n.parse::<i64>() {
                            Ok(Value::Integer(i))
                        } else if let Ok(f) = n.parse::<f64>() {
                            Ok(Value::Real(f))
                        } else {
                            Err(Error::Parse(format!("Invalid number: {}", n)))
                        }
                    }
                    sqlparser::ast::Value::SingleQuotedString(s) => Ok(Value::Text(s.clone())),
                    sqlparser::ast::Value::DoubleQuotedString(s) => Ok(Value::Text(s.clone())),
                    sqlparser::ast::Value::Null => Ok(Value::Null),
                    sqlparser::ast::Value::Placeholder(placeholder) => {
                        // Handle ? or $1 style placeholders
                        let index = if placeholder.starts_with('$') {
                            placeholder[1..].parse::<usize>()
                                .map_err(|_| Error::Parse(format!("Invalid placeholder: {}", placeholder)))?
                                .saturating_sub(1)
                        } else {
                            // For ? placeholders, we'd need to track position (TODO: full implementation)
                            return Err(Error::NotImplemented);
                        };

                        params.get(index)
                            .cloned()
                            .ok_or_else(|| Error::InvalidOperation(format!("Parameter index {} out of bounds", index + 1)))
                    }
                    _ => Err(Error::NotImplemented),
                }
            }
            _ => Err(Error::NotImplemented),
        }
    }
}

