pub mod error;
pub mod evaluator;

use std::sync::Arc;
use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::Type;
use sqlparser::ast::Statement;
use tracing::{debug, info};

use crate::config::Config;
use crate::executor::error::ExecutorError;
use crate::planner::{self, Operator};
use crate::parser;
use crate::storage::Database;
use crate::types::{Row, Value, Schema};

pub type Result<T> = std::result::Result<T, ExecutorError>;

pub(crate) struct Executor {
    db: Arc<parking_lot::RwLock<Database>>,
}

impl Executor {
    pub fn new(config: &Config) -> Self {
        Executor {
            db: Arc::new(parking_lot::RwLock::new(Database::new(config)))
        }
    }

    pub fn execute(&self, query: &str) -> Result<Vec<Response>> {
        debug!("parsing query");
        let stmts = parser::parse(query)?;

        if stmts.is_empty() {
            debug!("empty query");
            return Ok(vec![Response::EmptyQuery]);
        }

        info!(statement_count = stmts.len(), "parsed statements");

        let mut responses = Vec::new();
        for (idx, stmt) in stmts.iter().enumerate() {
            debug!(statement_idx = idx, "planning statement");

            // Handle DDL/DML/transactions directly (not via planner)
            let response = match stmt {
                Statement::StartTransaction { .. } => {
                    debug!("executing: start transaction");
                    Ok(Response::TransactionStart(Tag::new("BEGIN")))
                }
                Statement::Rollback { .. } => {
                    debug!("executing: rollback");
                    Ok(Response::TransactionEnd(Tag::new("ROLLBACK")))
                }
                Statement::Commit { .. } => {
                    debug!("executing: commit");
                    Ok(Response::TransactionEnd(Tag::new("COMMIT")))
                }
                Statement::CreateTable(ct) => {
                    debug!("executing: create table");
                    let (table_name, schema, _primary_key_col) = planner::extract_create_table(ct)?;
                    let mut db = self.db.write();
                    db.create_table(table_name.clone(), schema)
                        .map_err(|e| ExecutorError::Execution(e))?;
                    debug!(table = %table_name, "table created");
                    Ok(Response::EmptyQuery)
                }
                Statement::Insert(ins) => {
                    debug!("executing: insert");
                    let (table_name, row_exprs) = planner::extract_insert(ins)?;

                    // Get the schema from the table
                    let db = self.db.read();
                    let schema = db.get_schema(&table_name)
                        .map_err(|e| ExecutorError::Execution(e))?;
                    drop(db);

                    // Evaluate each row of expressions
                    let mut rows_to_insert = Vec::new();
                    for row_exprs_for_row in row_exprs {
                        let mut values = Vec::new();
                        // Create an empty row for schema context (INSERT doesn't reference existing columns)
                        let empty_row = Row::new(vec![]);
                        for expr in &row_exprs_for_row {
                            let val = evaluator::eval_expr(expr, &empty_row, &schema)?;
                            values.push(val);
                        }
                        rows_to_insert.push(Row::new(values));
                    }

                    // Insert the rows
                    let mut db = self.db.write();
                    for row in rows_to_insert {
                        db.insert_row(&table_name, row)
                            .map_err(|e| ExecutorError::Execution(e))?;
                    }
                    debug!(table = %table_name, "rows inserted");
                    Ok(Response::EmptyQuery)
                }
                Statement::CreateIndex(ci) => {
                    debug!("executing: create index");
                    let (table_name, column_name, index_type) = planner::extract_create_index(ci)?;

                    // Extract index name from the CREATE INDEX statement
                    let index_name = ci.name.as_ref()
                        .map(|name| name.0.iter()
                            .filter_map(|part| part.as_ident())
                            .map(|ident| ident.value.clone())
                            .collect::<Vec<_>>()
                            .join("."))
                        .unwrap_or_else(|| format!("idx_{}", table_name));

                    // Call database to create the secondary index
                    self.db.write()
                        .create_secondary_index(
                            index_name.clone(),
                            table_name.clone(),
                            column_name.clone(), 
                            index_type.clone(),
                        )
                        .map_err(|e| ExecutorError::Execution(e))?;

                    debug!(table = %table_name, column = %column_name, index_type = %index_type, index_name = %index_name, "secondary index created");
                    Ok(Response::EmptyQuery)
                }
                _ => {
                    let plan = planner::plan(stmt)?;
                    debug!(statement_idx = idx, plan = ?plan, "executing plan");
                    self.execute_plan(plan)
                }
            };

            responses.push(response?);
        }

        info!(response_count = responses.len(), "execution complete");
        Ok(responses)
    }

    fn execute_plan(&self, plan: Operator) -> Result<Response> {
        // Extract table name if available for schema lookup
        let table_name = self.extract_table_name(&plan);

        // Evaluate plan tree to get rows, then convert to Response
        let rows = self.execute_plan_rows(plan, table_name.clone())?;

        // Get the actual schema for proper column naming
        let schema = if let Some(table_name) = table_name {
            let db = self.db.read();
            db.get_schema(&table_name).ok()
        } else {
            None
        };

        rows_to_response(rows, schema)
    }

    fn extract_table_name(&self, plan: &Operator) -> Option<String> {
        match plan {
            Operator::TableScan { table } if table != "__constant__" => Some(table.clone()),
            Operator::IndexScan { table, .. } => Some(table.clone()),
            Operator::Filter { input, .. } => self.extract_table_name(input),
            Operator::Project { input, .. } => self.extract_table_name(input),
            Operator::Limit { input, .. } => self.extract_table_name(input),
            _ => None,
        }
    }

    fn execute_plan_rows(&self, plan: Operator, table_name: Option<String>) -> Result<Vec<Row>> {
        match plan {
            Operator::TableScan { table } if table == "__constant__" => {
                // Constant expression like SELECT 1
                debug!("executing constant scan");
                Ok(vec![Row::new(vec![Value::Int(1)])])
            }
            Operator::IndexScan { table, column, value } => {
                debug!(table = %table, column = %column, "executing index scan");
                let db = self.db.read();

                // Evaluate the value expression
                let schema = db.get_schema(&table)
                    .map_err(|e| ExecutorError::Execution(e))?;
                let empty_row = Row::new(vec![]);
                let lookup_val = evaluator::eval_expr(&value, &empty_row, &schema)?;

                // Convert value to u64 key for index lookup
                let key = match lookup_val {
                    Value::Int(n) => n as u64,
                    Value::Float(f) => f.to_bits(),
                    Value::String(s) => {
                        use std::collections::hash_map::DefaultHasher;
                        use std::hash::{Hash, Hasher};
                        let mut hasher = DefaultHasher::new();
                        s.hash(&mut hasher);
                        hasher.finish()
                    }
                    _ => return Err(ExecutorError::Execution("Cannot use NULL/Bool as index key".to_string())),
                };

                // Try to find a secondary index for this column, fall back to primary
                let result = db.search_secondary_index(&table, &column, key)
                    .or_else(|_| {
                        // If secondary index search fails, try primary index
                        debug!(column = %column, "secondary index not found or search failed, falling back to primary");
                        db.get_by_key(&table, key)
                    })
                    .map_err(|e| ExecutorError::Execution(e))?;

                // Fetch the row using the pointer if found
                match result {
                    Some(tuple_ptr) => {
                        let seg_id = tuple_ptr.segment_id;
                        let block_id = tuple_ptr.block_id;
                        let slot_id = tuple_ptr.slot_id;

                        // Read the block and extract the row
                        let block = db.read_block(seg_id, block_id)
                            .map_err(|e| ExecutorError::Execution(e))?;

                        if let Some(tuple_bytes) = block.read_tuple(slot_id) {
                            let (row, _): (Row, usize) = bincode::decode_from_slice(tuple_bytes, bincode::config::standard())
                                .map_err(|e| ExecutorError::Execution(format!("Deserialization error: {}", e)))?;
                            Ok(vec![row])
                        } else {
                            Ok(Vec::new())
                        }
                    }
                    None => {
                        debug!("key not found in any index");
                        Ok(Vec::new())
                    }
                }
            }
            Operator::TableScan { table } => {
                debug!(table = %table, "executing table scan");
                let db = self.db.read();
                let rows = db.scan_table(&table)
                    .map_err(|e| ExecutorError::Execution(e))?;
                // Note: Schema information is lost here, but will be recovered
                // in Project when needed via the actual table schema from DB
                Ok(rows)
            }
            Operator::Filter { input, predicate } => {
                debug!("executing filter");
                let rows = self.execute_plan_rows(*input, table_name)?;
                let schema = self.infer_schema(&rows);

                let filtered = rows
                    .into_iter()
                    .filter(|row| {
                        match evaluator::eval_expr(&predicate, row, &schema) {
                            Ok(Value::Bool(true)) => true,
                            Ok(Value::Bool(false)) => false,
                            Ok(Value::Null) => false,
                            _ => false,
                        }
                    })
                    .collect();
                Ok(filtered)
            }
            Operator::Project { input, columns } => {
                debug!("executing projection with {} columns", columns.len());
                let rows = self.execute_plan_rows(*input, table_name.clone())?;
                // Try to use actual table schema if available
                let schema = if let Some(table_name) = &table_name {
                    let db = self.db.read();
                    db.get_schema(table_name).unwrap_or_else(|_| self.infer_schema(&rows))
                } else {
                    self.infer_schema(&rows)
                };

                // Expand wildcards to actual column names
                let expanded_columns = columns.iter()
                    .flat_map(|col_expr| {
                        match col_expr {
                            sqlparser::ast::Expr::Identifier(ident) if &ident.value == "*" => {
                                // Replace wildcard with actual column expressions
                                schema.columns.iter()
                                    .map(|col| sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new(&col.name)))
                                    .collect::<Vec<_>>()
                            }
                            _ => vec![col_expr.clone()]
                        }
                    })
                    .collect::<Vec<_>>();

                let projected: Result<Vec<Row>> = rows
                    .iter()
                    .map(|row| {
                        let mut new_values = Vec::new();
                        for col_expr in &expanded_columns {
                            let val = evaluator::eval_expr(col_expr, row, &schema)?;
                            new_values.push(val);
                        }
                        Ok(Row::new(new_values))
                    })
                    .collect();
                projected
            }
            Operator::Aggregate { input, group_by: _, aggregates: _ } => {
                debug!("executing aggregate");
                let _rows = self.execute_plan_rows(*input, table_name)?;
                // TODO: Implement aggregation
                Ok(Vec::new())
            }
            Operator::Limit { input, limit, offset } => {
                debug!("executing limit {} offset {:?}", limit, offset);
                let rows = self.execute_plan_rows(*input, table_name)?;
                let skip = offset.unwrap_or(0) as usize;
                Ok(rows.into_iter()
                    .skip(skip)
                    .take(limit as usize)
                    .collect())
            }
        }
    }

    fn infer_schema(&self, rows: &[Row]) -> Schema {
        // For now, create a schema with generic column names
        if rows.is_empty() {
            return Schema::new(Vec::new());
        }

        let num_cols = rows[0].len();
        let mut columns = Vec::new();
        for i in 0..num_cols {
            columns.push(crate::types::Column {
                name: format!("col{}", i),
                data_type: crate::types::DataType::Int,
                is_primary_key: false,
            });
        }
        Schema::new(columns)
    }
}

fn rows_to_response(rows: Vec<Row>, schema: Option<Schema>) -> Result<Response> {
    // Convert Row data to pgwire Response
    if rows.is_empty() {
        return Ok(Response::EmptyQuery);
    }

    // Build column metadata for pgwire response
    let row_len = rows[0].len();
    let mut field_infos = Vec::new();

    if let Some(schema) = &schema {
        // Use actual column names from schema
        for col in &schema.columns {
            let pgwire_type = match col.data_type {
                crate::types::DataType::Int => Type::INT4,
                crate::types::DataType::Float => Type::FLOAT8,
                crate::types::DataType::String => Type::VARCHAR,
                crate::types::DataType::Bool => Type::BOOL,
                crate::types::DataType::Null => Type::UNKNOWN,
                crate::types::DataType::Extension { .. } => Type::UNKNOWN,
            };
            field_infos.push(FieldInfo::new(
                col.name.clone().into(),
                None,
                None,
                pgwire_type,
                FieldFormat::Text,
            ));
        }
    } else {
        // Fall back to generic names if no schema available
        for i in 0..row_len {
            field_infos.push(FieldInfo::new(
                format!("?column?{}", i).into(),
                None,
                None,
                Type::INT4,
                FieldFormat::Text,
            ));
        }
    }

    let schema = Arc::new(field_infos);
    let schema_ref = schema.clone();

    // Encode rows
    let mut encoded_rows = Vec::new();
    for row in rows {
        let mut encoder = DataRowEncoder::new(schema_ref.clone());
        for value in &row.values {
            match value {
                Value::Int(n) => {
                    encoder.encode_field(&(*n as i32))
                        .map_err(|e| ExecutorError::Execution(format!("Encoding error: {:?}", e)))?;
                }
                Value::Float(f) => {
                    encoder.encode_field(f)
                        .map_err(|e| ExecutorError::Execution(format!("Encoding error: {:?}", e)))?;
                }
                Value::String(s) => {
                    encoder.encode_field(s)
                        .map_err(|e| ExecutorError::Execution(format!("Encoding error: {:?}", e)))?;
                }
                Value::Bool(b) => {
                    encoder.encode_field(b)
                        .map_err(|e| ExecutorError::Execution(format!("Encoding error: {:?}", e)))?;
                }
                Value::Null => {
                    encoder.encode_field(&None::<i32>)
                        .map_err(|e| ExecutorError::Execution(format!("Encoding error: {:?}", e)))?;
                }
                Value::Extension { type_oid, .. } => {
                    // Extension values cannot be directly serialized to pgwire
                    // They require the TypeExtension trait for proper encoding
                    debug!("skipping extension value (type_oid: {})", type_oid);
                    encoder.encode_field(&None::<i32>)
                        .map_err(|e| ExecutorError::Execution(format!("Encoding error: {:?}", e)))?;
                }
            }
        }
        encoded_rows.push(encoder.finish());
    }

    let data_row_stream = stream::iter(encoded_rows);
    Ok(Response::Query(QueryResponse::new(schema, data_row_stream)))
}
