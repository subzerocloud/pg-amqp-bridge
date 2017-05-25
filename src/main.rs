extern crate env_logger;
extern crate pgsql_amqp_bridge as bridge;

use std::env;
use bridge::start_bridge;

#[derive(Debug, Clone)]
struct Config {
  postgresql_uri: String,
  amqp_host_port: String,
  bridge_channels: String,
}

impl Config {
  fn new() -> Config {
    Config {
      postgresql_uri: env::var("POSTGRESQL_URI").expect("POSTGRESQL_URI environment variable must be defined"),
      amqp_host_port: env::var("AMQP_HOST_PORT").expect("AMQP_HOST_PORT environment variable must be defined"),
      bridge_channels: env::var("BRIDGE_CHANNELS").expect("BRIDGE_CHANNELS environment variable must be defined"),
    }
  }
}

fn main() {
  let config = Config::new();
  if let Err(_) = env_logger::init() {
    panic!("Could not initialize logger");
  };
 start_bridge(&config.amqp_host_port, &config.postgresql_uri, &config.bridge_channels);
}
