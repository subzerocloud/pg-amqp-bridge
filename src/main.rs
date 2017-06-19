extern crate env_logger;
extern crate pg_amqp_bridge as bridge;

use std::env;
use bridge::Bridge;

#[derive(Debug, Clone)]
struct Config {
  postgresql_uri: String,
  amqp_uri: String,
  bridge_channels: String,
}

impl Config {
  fn new() -> Config {
    Config {
      postgresql_uri: env::var("POSTGRESQL_URI").expect("POSTGRESQL_URI environment variable must be defined"),
      amqp_uri: env::var("AMQP_URI").expect("AMQP_URI environment variable must be defined"),
      bridge_channels: env::var("BRIDGE_CHANNELS").expect("BRIDGE_CHANNELS environment variable must be defined"),
    }
  }
}

fn main() {
  env_logger::init().unwrap();
  let config = Config::new();
  Bridge::new().start(&config.amqp_uri, &config.postgresql_uri, &config.bridge_channels);
}
