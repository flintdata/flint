use flintdb::config::Config;
use flintdb::server::Server;

#[tokio::main]
pub async fn main() {
    let config = Config::from_args();
    let server = Server::new(config);
    server.start().await;
}