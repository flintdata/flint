use std::sync::Arc;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;
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
