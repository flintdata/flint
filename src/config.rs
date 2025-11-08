pub struct Config {
    pub(crate) bind_addr: String,
    pub(crate) port: u16,
    #[cfg(feature = "extensions")]
    pub(crate) load_all_extensions: bool,
    #[cfg(feature = "extensions")]
    pub(crate) enabled_extensions: Vec<String>,
}

impl Config {
    pub fn from_args() -> Self {
        Config {
            bind_addr: "127.0.0.1".to_string(),
            port: 5432,
            #[cfg(feature = "extensions")]
            load_all_extensions: false,
            #[cfg(feature = "extensions")]
            enabled_extensions: vec!["point-ext".into()],
        }
    }
}

