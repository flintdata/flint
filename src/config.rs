pub struct Config {
    pub(crate) bind_addr: String,
    pub(crate) port: u16,
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

