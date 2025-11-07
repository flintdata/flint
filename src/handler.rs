use std::fmt::Debug;
use std::sync::Arc;
use async_trait::async_trait;
use futures::Sink;
use pgwire::api::{ClientInfo, ClientPortalStore, NoopHandler, PgWireServerHandlers};
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::Response;
use pgwire::error::PgWireResult;
use pgwire::messages::PgWireBackendMessage;

use crate::executor::Executor;

pub(crate) struct HandlerFactory {
    handler: Arc<Handler>
}

impl HandlerFactory {
    pub fn new() -> Self {
        let executor = Arc::new(Executor::new());
        HandlerFactory {
            handler: Arc::new(Handler { executor })
        }
    }
}

impl PgWireServerHandlers for HandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        Arc::new(NoopHandler)
    }
}

struct Handler {
    executor: Arc<Executor>,
}

#[async_trait]
impl SimpleQueryHandler for Handler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        pgwire::error::PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        println!("Received query: {:?}", query);
        self.executor.execute(query).map_err(|e| e.into())
    }
}
