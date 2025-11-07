pub mod error;

use std::sync::Arc;
use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::Type;

use crate::executor::error::ExecutorError;
use crate::planner::{self, Plan};
use crate::parser;

pub type Result<T> = std::result::Result<T, ExecutorError>;

pub(crate) struct Executor {
    // storage: Arc<StorageEngine>
}

impl Executor {
    pub fn new() -> Self {
        Executor {}
    }

    pub fn execute(&self, query: &str) -> Result<Vec<Response>> {
        let stmts = parser::parse(query)?;

        if stmts.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        let mut responses = Vec::new();
        for stmt in stmts {
            let plan = planner::plan(&stmt)?;
            let response = self.execute_plan(plan)?;
            responses.push(response);
        }

        Ok(responses)
    }

    fn execute_plan(&self, plan: Plan) -> Result<Response> {
        match plan {
            Plan::StartTransaction => Ok(Response::TransactionStart(Tag::new("BEGIN"))),
            Plan::Rollback => Ok(Response::TransactionEnd(Tag::new("ROLLBACK"))),
            Plan::Commit => Ok(Response::TransactionEnd(Tag::new("COMMIT"))),
            Plan::SelectOne => {
                let f1 = FieldInfo::new(
                    "?column?".into(),
                    None,
                    None,
                    Type::INT4,
                    FieldFormat::Text,
                );
                let schema = Arc::new(vec![f1]);
                let schema_ref = schema.clone();

                let mut encoder = DataRowEncoder::new(schema_ref.clone());
                encoder.encode_field(&1i32)
                    .map_err(|e| ExecutorError::Execution(format!("Encoding error: {:?}", e)))?;
                let row = encoder.finish();

                let data_row_stream = stream::iter(vec![row]);
                Ok(Response::Query(QueryResponse::new(schema, data_row_stream)))
            }
            Plan::Unsupported(msg) => {
                Err(ExecutorError::UnsupportedStatement(msg))
            }
        }
    }
}