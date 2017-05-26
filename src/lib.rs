extern crate amq_protocol;
extern crate fallible_iterator;
extern crate futures;
extern crate lapin_futures as lapin;
#[macro_use] extern crate log;
extern crate postgres;
extern crate tokio_core;

use fallible_iterator::FallibleIterator;
use futures::*;
use lapin::client::*;
use lapin::channel::*;
use lapin::client::Client;
use postgres::{Connection, TlsMode};
use std::net::SocketAddr;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;
use std::string::String;
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;

#[derive(PartialEq, Debug)]
struct Bridge{
  pg_channel: String,
  amqp_entity: String,
}

#[derive(Debug)]
struct Envelope{
  pg_channel: String,
  routing_key: String,
  message: String,
  amqp_entity: String,
}

const SEPARATOR: char = '|';

pub fn start_bridge(amqp_host_port: &String, pg_uri: &String, bridge_channels: &String){
  let amqp_addr: SocketAddr = amqp_host_port.parse().expect("amqp_host_port should be in format '127.0.0.1:5672'");
  let bridges = parse_bridge_channels(bridge_channels);

  let (sender, receiver): (Sender<Envelope>, Receiver<Envelope>) = channel();

  for bridge in bridges{
    spawn_pg_listener(pg_uri.clone(), bridge, sender.clone());
  }

  amqp_publish(amqp_addr, receiver);
}

fn parse_bridge_channels(bridge_channels: &str) -> Vec<Bridge>{
  let mut bridges: Vec<Bridge> = Vec::new();
  let str_bridges: Vec<Vec<&str>> = bridge_channels.split(",").map(|s| s.split(":").collect()).collect();
  for i in 0..str_bridges.len(){
    bridges.push(Bridge{pg_channel: str_bridges[i][0].trim().to_string(),
                        amqp_entity: str_bridges[i].get(1).unwrap_or(&"").trim().to_string()
                 });
  }
  let flt_bs : Vec<Bridge> = bridges.into_iter().filter(|x| !x.pg_channel.is_empty()).collect();
  if flt_bs.len() == 0 { 
    panic!("No postgresql channel specified in \"{}\"", bridge_channels) 
  }
  flt_bs
}

fn spawn_pg_listener(pg_uri: String, bridge: Bridge, sender: Sender<Envelope>){
  thread::spawn(move||{
    let pg_conn = Connection::connect(pg_uri, TlsMode::None).expect("Could not connect to PostgreSQL");
    let listen_command = format!("LISTEN {}", bridge.pg_channel);
    pg_conn.execute(listen_command.as_str(), &[]).expect("Could not send LISTEN");
    let notifications = pg_conn.notifications();
    let mut it = notifications.blocking_iter();
    loop {
      match it.next() {
        Ok(Some(notification)) => {
          let (routing_key, message) = parse_notification(&notification.payload);
          let envelope = Envelope{pg_channel: bridge.pg_channel.clone(),
                                  routing_key: routing_key.to_string(),
                                  message: message.to_string(),
                                  amqp_entity: bridge.amqp_entity.clone()};
          debug!("Sent: {:?}", envelope);
          if let Err(e) = sender.send(envelope) {
            error!("Could not send: {:?}", e);
          };
        },
        Err(err) => panic!("Got err {:?}", err),
        _ => panic!("Unexpected state.")
      }
    }
  });
  ()
}

fn parse_notification(payload: &str) -> (&str, &str){
  let v: Vec<&str> = payload.splitn(2, SEPARATOR).map(|x| x.trim()).collect();
  if v.len() > 1 {
    (v[0], v[1])
  } else {
    ("", v[0])
  }
}

fn amqp_publish(amqp_addr: SocketAddr, receiver: Receiver<Envelope>){
  let mut core = Core::new().expect("Could not create event loop");
  let handle = core.handle();
  core.run(
    TcpStream::connect(&amqp_addr, &handle)
    .and_then(|stream| Client::connect(stream, &ConnectionOptions::default()))
    .and_then(|client| client.create_channel())
    .and_then(|channel| {
      println!("Waiting for notifications...");
      loop {
        let envelope = match receiver.recv() {
          Ok(x) => x,
          Err(e) => {
            error!("Could not receive: {:?}", e); 
            continue
          }
        };
        debug!("Received: {:?}", envelope);
        println!("Forwarding {:?} from pg channel {:?} to exchange {:?} with routing key {:?} ",
                 envelope.message, envelope.pg_channel, envelope.amqp_entity, envelope.routing_key);
        channel.basic_publish(envelope.amqp_entity.as_str(), envelope.routing_key.as_str(), format!("{:?}", envelope.message).as_bytes(),
                              &BasicPublishOptions::default(), BasicProperties::default());
      }
      Ok(channel)
    })
  ).expect("Could not publish message to amqp server");
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn parse_notification_works() {
    assert!(("my_key", "A message") == parse_notification("my_key|A message"));
    assert!(("my_key", "A message") == parse_notification("  my_key  |  A message  "));
    assert!(("my_key", "A message|Rest of message") == parse_notification("my_key|A message|Rest of message"));
    assert!(("", "my_key##A message") == parse_notification("my_key##A message"));
    assert!(("", "A message") == parse_notification("A message"));
    assert!(("", "") == parse_notification(""));
    assert!(("mý_kéý", "A mésságé") == parse_notification("mý_kéý|A mésságé"));
  }

  #[test]
  fn parse_bridge_channels_works() {
    assert!(vec![Bridge{pg_channel: "pgchannel1".to_string(), amqp_entity: "exchange1".to_string()}] 
            == parse_bridge_channels("pgchannel1:exchange1"));
    assert!(vec![
              Bridge{pg_channel: "pgchannel1".to_string(), amqp_entity: "exchange1".to_string()},
              Bridge{pg_channel: "pgchannel2".to_string(), amqp_entity: "exchange2".to_string()}
            ] == parse_bridge_channels("pgchannel1:exchange1,pgchannel2:exchange2"));
    assert!(vec![
              Bridge{pg_channel: "pgchannel1".to_string(), amqp_entity: "exchange1".to_string()},
              Bridge{pg_channel: "pgchannel2".to_string(), amqp_entity: "exchange2".to_string()},
              Bridge{pg_channel: "pgchannel3".to_string(), amqp_entity: "exchange3".to_string()}
            ] == parse_bridge_channels(" pgchannel1 : exchange1 , pgchannel2 : exchange2 , pgchannel3 : exchange3 "));
    assert!(vec![
              Bridge{pg_channel: "pgchannel1".to_string(), amqp_entity: "".to_string()},
              Bridge{pg_channel: "pgchannel2".to_string(), amqp_entity: "".to_string()},
              Bridge{pg_channel: "pgchannel3".to_string(), amqp_entity: "exchange3".to_string()}
            ] == parse_bridge_channels("pgchannel1,pgchannel2:,pgchannel3:exchange3,"));
  }

  use std::panic::catch_unwind;

  #[test]
  fn parse_bridge_channels_panics_if_no_pg_channel() {
    assert!(catch_unwind(|| parse_bridge_channels("   ")).is_err());
    assert!(catch_unwind(|| parse_bridge_channels(":exchange1")).is_err());
    assert!(catch_unwind(|| parse_bridge_channels(":exchange1,:exchange2")).is_err());
    assert!(catch_unwind(|| parse_bridge_channels(",:,:exchange3,,")).is_err());
  }
}
