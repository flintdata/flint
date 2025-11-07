use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, Sink, SinkExt};
use pgwire::api::{ClientInfo, ClientPortalStore, NoopHandler, PgWireServerHandlers, Type};
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

pub struct Config {
    bind_addr: String,
    port: u16,
}

impl Config {
    pub fn from_args() -> Self {
        // TODO: Parse command-line arguments
        Config {
            bind_addr: "127.0.0.1".to_string(),
            port: 5432,
        }
    }
}

struct Handler;

#[async_trait]
impl SimpleQueryHandler for Handler { async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
where
    C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    println!("Received query: {:?}", query);
    let resp = match query {
        "BEGIN;" => Response::TransactionStart(Tag::new("BEGIN")),
        "ROLLBACK;" => Response::TransactionEnd(Tag::new("ROLLBACK")),
        "COMMIT;" => Response::TransactionEnd(Tag::new("COMMIT")),
        "SELECT 1;" => {
            let f1 =
                FieldInfo::new("?column?".into(), None, None, Type::INT4, FieldFormat::Text);
            let schema = Arc::new(vec![f1]);
            let schema_ref = schema.clone();

            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            encoder.encode_field(&1i32)?;
            let row = encoder.finish();

            let data_row_stream = stream::iter(vec![row]);

            Response::Query(QueryResponse::new(schema, data_row_stream))
        }
        _ => Response::Error(Box::new(ErrorInfo::new(
            "FATAL".to_string(),
            "38003".to_string(),
            "Unsupported statement.".to_string(),
        ))),
    };

    Ok(vec![resp])
}
}

struct HandlerFactory {
    pub handler: Arc<Handler>
}

impl HandlerFactory {
    pub fn new() -> Self {
        Self { handler: Arc::new(Handler) }
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


pub struct Server {
    config: Config,
}

impl Server {
    pub fn new(config: Config) -> Self {
        Server { config }
    }

    pub async fn start(&self) {
        let factory = Arc::new(HandlerFactory::new());

        let server_addr = format!("{}:{}", self.config.bind_addr, self.config.port);
        let listener = TcpListener::bind(server_addr).await.unwrap();

        loop {
            let incoming_socket = listener.accept().await.unwrap();
            let factory_ref = factory.clone();
            tokio::spawn(async move {
                match process_socket(incoming_socket.0, None, factory_ref).await {
                    Ok(_) => println!("Connection closed cleanly"),
                    Err(e) => eprintln!("Connection error: {:?}", e),
                }
            });
        }
    }
}
