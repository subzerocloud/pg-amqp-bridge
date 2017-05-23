#[macro_use] extern crate log;
extern crate env_logger;

extern crate amq_protocol;
extern crate fallible_iterator;
extern crate futures;
extern crate lapin_futures as lapin;
extern crate postgres;
extern crate tokio_core;

use amq_protocol::types::FieldTable;
use fallible_iterator::FallibleIterator;
use futures::*;
use lapin::client::*;
use lapin::channel::*;
use postgres::{Connection, TlsMode};
use std::thread;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::string::String;
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;

mod config;

#[derive(Debug)]
struct Bridge{
  pg_channel: String,
  amqp_entity: String,
}

#[derive(Debug)]
struct Envelope{
  pg_channel: String,
  amqp_entity: String,
  payload: String,
}

fn parse_bridges(bridge_channels: &str) -> Vec<Bridge>{
  let mut bridges: Vec<Bridge> = Vec::new();
  let str_bridges: Vec<Vec<&str>> = bridge_channels.split(",").map(|s| s.split(":").collect()).collect();
  for i in 0..str_bridges.len(){
    bridges.push(Bridge{pg_channel: str_bridges[i][0].to_string(), amqp_entity: str_bridges[i][1].to_string()});
  }
  bridges
}

fn main() {
  let config = config::Config::new();
  env_logger::init().unwrap();

  let bridge_channels = &config.bridge_channels;
  let amqp_addr = config.amqp_host_port.parse().expect("amqp_host_port should be in format '127.0.0.1:5672'");
  let pg_uri = &config.postgresql_uri;

  let bridges = parse_bridges(bridge_channels);

  let mut core = Core::new().unwrap();
  let handle = core.handle();

  // Validate that amqp entities already exist, the "passive" attribute set to true does this
  for bridge in &bridges{
    let res = core.run( 
      TcpStream::connect(&amqp_addr, &handle)
      .and_then(|stream| 
       lapin::client::Client::connect(stream, &ConnectionOptions::default()))
      .and_then(|client|
       client.create_channel())
      .and_then(|channel| {
       channel.queue_declare(&bridge.amqp_entity, 
                             &QueueDeclareOptions{ 
                               passive: true, durable: false, exclusive: false, 
                               auto_delete: false, nowait: false}, 
                             FieldTable::new())
      })
    );
    if let Err(_) = res {
      panic!("AMQP entity {} doesn't exist", bridge.amqp_entity)
    }
  }

  let (tx, rx): (Sender<Envelope>, Receiver<Envelope>) = channel();

  // Spawn threads that communicates via channel with amqp main event loop
  for bridge in bridges{
    let thread_tx = tx.clone();
    let pg_uri = pg_uri.clone();
    thread::spawn(move|| {
      let pg_conn = Connection::connect(pg_uri, TlsMode::None).expect("Could not connect to PostgreSQL");
      let listen_command = format!("LISTEN {}", bridge.pg_channel);
      pg_conn.execute(listen_command.as_str(), &[]).expect("Could not send LISTEN");
      let notifications = pg_conn.notifications();
      let mut it = notifications.blocking_iter();
      loop {
        match it.next() {
          Ok(Some(notification)) => {
            let envelope = Envelope{pg_channel: bridge.pg_channel.clone(), payload: notification.payload, amqp_entity: bridge.amqp_entity.clone()};
            debug!("Sent: {:?}", envelope);
            thread_tx.send(envelope).unwrap();
          },
          Err(err) => panic!("Got err {:?}", err),
          _ => panic!("Unexpected state.")
        }
      }
    });
  }

  let _message = core.run(
    TcpStream::connect(&amqp_addr, &handle)
    .and_then(|stream| lapin::client::Client::connect(stream, &ConnectionOptions::default()))
    .and_then(|client| client.create_confirm_channel())
    .and_then(|channel| {
      println!("Waiting for notifications...");
      loop {
        let envelope = rx.recv().unwrap();
        debug!("Received: {:?}", envelope);
        println!("Forwarding {:?} from pg channel {:?} to amqp entity {:?}", envelope.payload, envelope.pg_channel, envelope.amqp_entity);
        channel.basic_publish(envelope.amqp_entity.as_str(), format!("{:?}", envelope.payload).as_bytes(), 
                              &BasicPublishOptions::default(), BasicProperties::default());
      }
      Ok(channel)
    })
  );
}
