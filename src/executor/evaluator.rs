use sqlparser::ast::{Expr, BinaryOperator};
use tracing::debug;

use crate::executor::error::ExecutorError;
use crate::types::{Row, Schema, Value};

pub type Result<T> = std::result::Result<T, ExecutorError>;

/// Evaluate a SQL expression against a row
pub fn eval_expr(expr: &Expr, row: &Row, schema: &Schema) -> Result<Value> {
    match expr {
        // Literals
        Expr::Value(val) => {
            match &val.value {
                sqlparser::ast::Value::Number(n, _) => {
                    // Try parsing as i64 first, then f64
                    if let Ok(i) = n.parse::<i64>() {
                        Ok(Value::Int(i))
                    } else if let Ok(f) = n.parse::<f64>() {
                        Ok(Value::Float(f))
                    } else {
                        Err(ExecutorError::Execution(format!("Invalid number: {}", n)))
                    }
                }
                sqlparser::ast::Value::SingleQuotedString(s) => Ok(Value::String(s.clone())),
                sqlparser::ast::Value::Boolean(b) => Ok(Value::Bool(*b)),
                sqlparser::ast::Value::Null => Ok(Value::Null),
                _ => Err(ExecutorError::Execution(format!(
                    "Unsupported value type: {:?}",
                    val.value
                ))),
            }
        }

        // Column reference
        Expr::Identifier(ident) => {
            let col_name = &ident.value;
            debug!(column = %col_name, "evaluating column reference");

            if let Some(idx) = schema.get_column_index(col_name) {
                row.get(idx)
                    .cloned()
                    .ok_or_else(|| ExecutorError::Execution(format!("Column index out of bounds: {}", col_name)))
            } else {
                Err(ExecutorError::Execution(format!(
                    "Column not found: {}",
                    col_name
                )))
            }
        }

        // Binary operations
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_expr(left, row, schema)?;
            let right_val = eval_expr(right, row, schema)?;
            eval_binary_op(&left_val, op, &right_val)
        }

        // Parenthesized expression
        Expr::Nested(inner) => eval_expr(inner, row, schema),

        // Wildcard (shouldn't reach here in typical evaluation)
        Expr::Wildcard(_) => Ok(Value::Null),

        _ => Err(ExecutorError::Execution(format!(
            "Unsupported expression: {:?}",
            expr
        ))),
    }
}

/// Evaluate a binary operation
fn eval_binary_op(left: &Value, op: &BinaryOperator, right: &Value) -> Result<Value> {
    use BinaryOperator::*;

    match op {
        // Comparison operators
        Eq => {
            let result = match (left, right) {
                (Value::Int(a), Value::Int(b)) => a == b,
                (Value::Float(a), Value::Float(b)) => a == b,
                (Value::Int(a), Value::Float(b)) => *a as f64 == *b,
                (Value::Float(a), Value::Int(b)) => *a == *b as f64,
                (Value::String(a), Value::String(b)) => a == b,
                (Value::Bool(a), Value::Bool(b)) => a == b,
                (Value::Null, _) | (_, Value::Null) => false, // NULL comparisons are false
                _ => return Err(ExecutorError::Execution(
                    "Type mismatch in comparison".to_string(),
                )),
            };
            Ok(Value::Bool(result))
        }

        NotEq => {
            let result = match (left, right) {
                (Value::Int(a), Value::Int(b)) => a != b,
                (Value::Float(a), Value::Float(b)) => a != b,
                (Value::Int(a), Value::Float(b)) => *a as f64 != *b,
                (Value::Float(a), Value::Int(b)) => *a != *b as f64,
                (Value::String(a), Value::String(b)) => a != b,
                (Value::Bool(a), Value::Bool(b)) => a != b,
                (Value::Null, _) | (_, Value::Null) => false,
                _ => return Err(ExecutorError::Execution(
                    "Type mismatch in comparison".to_string(),
                )),
            };
            Ok(Value::Bool(result))
        }

        Gt => {
            let result = match (left, right) {
                (Value::Int(a), Value::Int(b)) => a > b,
                (Value::Float(a), Value::Float(b)) => a > b,
                (Value::Int(a), Value::Float(b)) => *a as f64 > *b,
                (Value::Float(a), Value::Int(b)) => *a > *b as f64,
                (Value::String(a), Value::String(b)) => a > b,
                (Value::Null, _) | (_, Value::Null) => false,
                _ => return Err(ExecutorError::Execution(
                    "Type mismatch in comparison".to_string(),
                )),
            };
            Ok(Value::Bool(result))
        }

        Lt => {
            let result = match (left, right) {
                (Value::Int(a), Value::Int(b)) => a < b,
                (Value::Float(a), Value::Float(b)) => a < b,
                (Value::Int(a), Value::Float(b)) => (*a as f64) < *b,
                (Value::Float(a), Value::Int(b)) => *a < (*b as f64),
                (Value::String(a), Value::String(b)) => a < b,
                (Value::Null, _) | (_, Value::Null) => false,
                _ => return Err(ExecutorError::Execution(
                    "Type mismatch in comparison".to_string(),
                )),
            };
            Ok(Value::Bool(result))
        }

        GtEq => {
            let result = match (left, right) {
                (Value::Int(a), Value::Int(b)) => a >= b,
                (Value::Float(a), Value::Float(b)) => a >= b,
                (Value::Int(a), Value::Float(b)) => *a as f64 >= *b,
                (Value::Float(a), Value::Int(b)) => *a >= *b as f64,
                (Value::String(a), Value::String(b)) => a >= b,
                (Value::Null, _) | (_, Value::Null) => false,
                _ => return Err(ExecutorError::Execution(
                    "Type mismatch in comparison".to_string(),
                )),
            };
            Ok(Value::Bool(result))
        }

        LtEq => {
            let result = match (left, right) {
                (Value::Int(a), Value::Int(b)) => a <= b,
                (Value::Float(a), Value::Float(b)) => a <= b,
                (Value::Int(a), Value::Float(b)) => (*a as f64) <= *b,
                (Value::Float(a), Value::Int(b)) => *a <= (*b as f64),
                (Value::String(a), Value::String(b)) => a <= b,
                (Value::Null, _) | (_, Value::Null) => false,
                _ => return Err(ExecutorError::Execution(
                    "Type mismatch in comparison".to_string(),
                )),
            };
            Ok(Value::Bool(result))
        }

        // Arithmetic operators
        Plus => {
            match (left, right) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + *b as f64)),
                _ => Err(ExecutorError::Execution("Type mismatch in +".to_string())),
            }
        }

        Minus => {
            match (left, right) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a - *b as f64)),
                _ => Err(ExecutorError::Execution("Type mismatch in -".to_string())),
            }
        }

        Multiply => {
            match (left, right) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a * *b as f64)),
                _ => Err(ExecutorError::Execution("Type mismatch in *".to_string())),
            }
        }

        Divide => {
            match (left, right) {
                (Value::Int(a), Value::Int(b)) => {
                    if *b == 0 {
                        Err(ExecutorError::Execution("Division by zero".to_string()))
                    } else {
                        Ok(Value::Int(a / b))
                    }
                }
                (Value::Float(a), Value::Float(b)) => {
                    if *b == 0.0 {
                        Err(ExecutorError::Execution("Division by zero".to_string()))
                    } else {
                        Ok(Value::Float(a / b))
                    }
                }
                (Value::Int(a), Value::Float(b)) => {
                    if *b == 0.0 {
                        Err(ExecutorError::Execution("Division by zero".to_string()))
                    } else {
                        Ok(Value::Float(*a as f64 / b))
                    }
                }
                (Value::Float(a), Value::Int(b)) => {
                    if *b == 0 {
                        Err(ExecutorError::Execution("Division by zero".to_string()))
                    } else {
                        Ok(Value::Float(a / *b as f64))
                    }
                }
                _ => Err(ExecutorError::Execution("Type mismatch in /".to_string())),
            }
        }

        // Logical operators
        And => {
            match (left, right) {
                (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(*a && *b)),
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(ExecutorError::Execution("Type mismatch in AND".to_string())),
            }
        }

        Or => {
            match (left, right) {
                (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(*a || *b)),
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(ExecutorError::Execution("Type mismatch in OR".to_string())),
            }
        }

        _ => Err(ExecutorError::Execution(format!(
            "Unsupported binary operator: {:?}",
            op
        ))),
    }
}