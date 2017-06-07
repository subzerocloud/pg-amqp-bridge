extern crate amqp;
extern crate fallible_iterator;
extern crate postgres;

use amqp::{Session, Basic, protocol, Channel};
use fallible_iterator::FallibleIterator;
use postgres::*;
use std::default::Default;
use std::thread;
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
  let bridges = parse_bridge_channels(bridge_channels);

  let mut children = Vec::new();

  let mut session = Session::open_url(amqp_host_port.as_str()).unwrap();

  let mut i = 0;
  for bridge in bridges{
    i += 1;
    children.push(spawn_listener_publisher(session.open_channel(i).unwrap(), pg_uri.clone(), bridge));
  }

  for child in children{
    let _ = child.join();
  }
}

fn spawn_listener_publisher(mut channel: Channel, pg_uri: String, bridge: Bridge) -> JoinHandle<()>{
  thread::spawn(move ||{
    let pg_conn = Connection::connect(pg_uri, TlsMode::None).expect("Could not connect to PostgreSQL");

    let listen_command = format!("LISTEN {}", bridge.pg_channel);
    pg_conn.execute(listen_command.as_str(), &[]).expect("Could not send LISTEN");

    println!("Listening on {}...", bridge.pg_channel);

    let notifications = pg_conn.notifications();
    let mut it = notifications.blocking_iter();
    while let Ok(Some(notification)) = it.next() {
      let (routing_key, message) = parse_notification(&notification.payload);
      let envelope = Envelope{pg_channel: &bridge.pg_channel,
                              routing_key: &routing_key,
                              message: &message,
                              amqp_entity: &bridge.amqp_entity};
      channel.basic_publish(envelope.amqp_entity, envelope.routing_key, true, false,
                            protocol::basic::BasicProperties{ content_type: Some("text".to_string()), ..Default::default()},
                            envelope.message.as_bytes().to_vec()).ok().expect("Failed publishing");
      println!("Forwarding {:?} from pg channel {:?} to exchange {:?} with routing key {:?} ",
               envelope.message, envelope.pg_channel, envelope.amqp_entity, envelope.routing_key);
    }
  })
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
