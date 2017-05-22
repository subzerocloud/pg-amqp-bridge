use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub postgresql_uri: String,
    pub amqp_host_port: String,
    pub bridge_channels: String,
}

impl Config {
    pub fn new() -> Config {
        Config {
            postgresql_uri: env::var("POSTGRESQL_URI").expect("POSTGRESQL_URI environment variable must be defined"),
            amqp_host_port: env::var("AMQP_HOST_PORT").expect("AMQP_HOST_PORT environment variable must be defined"),
            bridge_channels: env::var("BRIDGE_CHANNELS").expect("BRIDGE_CHANNELS environment variable must be defined"),
        }
    }
}
