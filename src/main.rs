#[macro_use] extern crate log;
extern crate env_logger;

extern crate amq_protocol;
extern crate fallible_iterator;
extern crate futures;
extern crate lapin_futures as lapin;
extern crate postgres;
extern crate tokio_core;

mod config;
mod bridge;

fn main() {
  let config = config::Config::new();
  if let Err(_) = env_logger::init() {
    panic!("Could not initialize logger");
  };
  bridge::start_bridge(&config.amqp_host_port, &config.postgresql_uri, &config.bridge_channels);
}
