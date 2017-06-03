extern crate amq_protocol;
extern crate fallible_iterator;
extern crate futures;
extern crate lapin_futures as lapin;
extern crate tokio_core;
extern crate tokio_postgres;

use futures::*;
use lapin::client::*;
use lapin::channel::*;
use lapin::client::Client;
use std::net::SocketAddr;
use std::thread;
use std::string::String;
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;
use tokio_postgres::*;
use std::io::{Error, ErrorKind};
use std::thread::JoinHandle;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct Bridge{
  pg_channel: String,
  amqp_entity: String,
}

#[derive(Debug)]
struct Envelope<'a>{
  pg_channel: &'a str,
  routing_key: &'a str,
  message: &'a str,
  amqp_entity: &'a str,
}

const SEPARATOR: char = '|';

pub fn start_bridge(amqp_host_port: &String, pg_uri: &String, bridge_channels: &String){
  let amqp_addr: SocketAddr = amqp_host_port.parse().expect("amqp_host_port should be in format '127.0.0.1:5672'");
  let bridges = parse_bridge_channels(bridge_channels);

  let mut children = Vec::new();
  for bridge in bridges{
    children.push(spawn_listener_publisher(amqp_addr.clone(), pg_uri.clone(), bridge));
  }

  for child in children{
    let _ = child.join();
  }
}

fn parse_bridge_channels(bridge_channels: &str) -> Vec<Bridge>{
  let mut bridges: Vec<Bridge> = Vec::new();
  let str_bridges: Vec<Vec<&str>> = bridge_channels.split(",").map(|s| s.split(":").collect()).collect();
  for i in 0..str_bridges.len(){
    bridges.push(Bridge{pg_channel: str_bridges[i][0].trim().to_string(),
                        amqp_entity: str_bridges[i].get(1).unwrap_or(&"").trim().to_string()
                 });
  }
  let mut flt_bs : Vec<Bridge> = bridges.into_iter().filter(|x| !x.pg_channel.is_empty()).collect();
  if flt_bs.len() == 0 {
    panic!("No postgresql channel specified in \"{}\"", bridge_channels)
  }
  flt_bs.sort();
  flt_bs.dedup_by(|a, b| a.pg_channel == b.pg_channel);
  flt_bs
}

fn parse_notification(payload: &str) -> (&str, &str){
  let v: Vec<&str> = payload.splitn(2, SEPARATOR).map(|x| x.trim()).collect();
  if v.len() > 1 {
    (v[0], v[1])
  } else {
    ("", v[0])
  }
}

fn spawn_listener_publisher(amqp_addr: SocketAddr, pg_uri: String, bridge: Bridge) -> JoinHandle<()>{
  thread::spawn(move ||{
    let mut core = Core::new().expect("Could not create event loop");
    let handle = core.handle();
    let br = bridge.clone();
    core.run(
      TcpStream::connect(&amqp_addr, &handle)
      .and_then(|stream| Client::connect(stream, &ConnectionOptions::default()))
      .and_then(|client| client.create_channel())
      .and_then(|channel| {
        println!("Listening on {}...", bridge.pg_channel);
        Connection::connect(pg_uri, TlsMode::None, &handle)
        .then(|conn| 
          conn.unwrap().batch_execute(format!("LISTEN {}", &bridge.pg_channel).as_str())
          .map_err(|_|Error::new(ErrorKind::Other, ""))
        )
        .and_then(|conn|
          conn.notifications().map_err(|_|Error::new(ErrorKind::Other, ""))
          .for_each(move |n|{
            let (routing_key, message) = parse_notification(&n.payload);
            let envelope = Envelope{pg_channel: &br.pg_channel,
                                    routing_key: &routing_key,
                                    message: &message,
                                    amqp_entity: &br.amqp_entity};
            println!("Forwarding {:?} from pg channel {:?} to exchange {:?} with routing key {:?} ",
                     envelope.message, envelope.pg_channel, envelope.amqp_entity, envelope.routing_key);
            channel.basic_publish(envelope.amqp_entity, envelope.routing_key, envelope.message.as_bytes(),
                                  &BasicPublishOptions::default(), BasicProperties::default())
            .and_then(|_|Ok(()))
          })
        )
      })
    ).expect("Could not publish message to amqp server");
  })
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
    assert!(vec![
              Bridge{pg_channel: "pgchannel1".to_string(), amqp_entity: "".to_string()},
            ] == parse_bridge_channels("pgchannel1,pgchannel1:exchange2,pgchannel1:exchange3,"));
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
