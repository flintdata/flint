use std::sync::Arc;

use pgwire::tokio::process_socket;
use tokio::net::TcpListener;
use tracing::{debug, error, info, span, Level};

use crate::config::Config;
use crate::handler::HandlerFactory;

pub struct Server {
    config: Config,
}

impl Server {
    pub fn new(config: Config) -> Self {
        Server { config }
    }

    pub async fn start(&self) {
        let factory = Arc::new(HandlerFactory::new(&self.config));

        let server_addr = format!("{}:{}", self.config.bind_addr, self.config.port);
        let listener = TcpListener::bind(&server_addr).await.unwrap();

        info!(addr = %server_addr, "server listening");

        loop {
            let incoming_socket = listener.accept().await.unwrap();
            let client_addr = incoming_socket.1;

            let factory_ref = factory.clone();
            tokio::spawn(async move {
                let span = span!(Level::INFO, "connection", client_addr = %client_addr);
                let _enter = span.enter();

                info!("new connection");

                match process_socket(incoming_socket.0, None, factory_ref).await {
                    Ok(_) => debug!("connection closed"),
                    Err(e) => error!(error = %e, "connection error"),
                }
            });
        }
    }
}
