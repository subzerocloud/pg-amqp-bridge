extern crate amqp;
extern crate fallible_iterator;
extern crate postgres;

use amqp::{Session, Basic, protocol, Channel, Table};
use fallible_iterator::FallibleIterator;
use postgres::*;
use std::default::Default;
use std::thread;
use std::thread::JoinHandle;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
enum Type {
  Exchange,
  Queue
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct Binding{
  pg_channel: String,
  amqp_entity: String,
  amqp_entity_type: Option<Type>
}

const SEPARATOR: char = '|';

pub fn start_bridge(amqp_uri: &String, pg_uri: &String, bridge_channels: &String){
  let mut bindings = parse_bridge_channels(bridge_channels);
  let mut children = Vec::new();

  let mut session = Session::open_url(amqp_uri.as_str()).unwrap();
  let mut channel_id = 0;

  for binding in &mut bindings{
    channel_id += 1; let channel1 = session.open_channel(channel_id).unwrap();
    channel_id += 1; let channel2 = session.open_channel(channel_id).unwrap();
    let amqp_entity_type = amqp_entity_type(channel1, channel2, &binding.amqp_entity);
    if amqp_entity_type.is_none(){
      panic!("The amqp entity {:?} doesn't exist", binding.amqp_entity);
    }else{
      binding.amqp_entity_type = amqp_entity_type;
    }
  }

  for binding in bindings{
    channel_id += 1; let channel = session.open_channel(channel_id).unwrap();
    children.push(spawn_listener_publisher(channel, pg_uri.clone(), binding));
  }

  for child in children{
    let _ = child.join();
  }
}

fn spawn_listener_publisher(mut channel: Channel, pg_uri: String, binding: Binding) -> JoinHandle<()>{
  thread::spawn(move ||{
    let pg_conn = Connection::connect(pg_uri, TlsMode::None).expect("Could not connect to PostgreSQL");

    let listen_command = format!("LISTEN {}", binding.pg_channel);
    pg_conn.execute(listen_command.as_str(), &[]).unwrap();

    println!("Listening on {}...", binding.pg_channel);

    let notifications = pg_conn.notifications();
    let mut it = notifications.blocking_iter();

    let amqp_entity_type = binding.amqp_entity_type.unwrap();

    while let Ok(Some(notification)) = it.next() {
      let (routing_key, message) = parse_notification(&notification.payload);
      let (exchange, key) =  if amqp_entity_type == Type::Exchange {
                               (binding.amqp_entity.as_str(), routing_key)
                             } else {
                               ("", binding.amqp_entity.as_str())
                             };
      channel.basic_publish(exchange, key, true, false,
                            protocol::basic::BasicProperties{ content_type: Some("text".to_string()), ..Default::default()},
                            message.as_bytes().to_vec()).unwrap();
      println!("Forwarding {:?} from pg channel {:?} to {:?} {:?} with routing key {:?} ",
               message, binding.pg_channel, amqp_entity_type, binding.amqp_entity, routing_key);
    }
  })
}

/*
 * Finds the amqp entity type(Queue or Exchange) using two channels because currently rust-amqp hangs up when doing exchange_declare
 * and queue_declare on the samme channel.
 * It does this with amqp "passive" set to true.
*/
fn amqp_entity_type(mut channel1: Channel, mut channel2: Channel, amqp_entity: &String) -> Option<Type>{
  let opt_queue_type = channel1.queue_declare(amqp_entity.clone(), true, false, false, false, false, Table::new())
                       .map(|_| Type::Queue).ok();
  let opt_exchange_type = channel2.exchange_declare(amqp_entity.clone(), "".to_string(), true, false, false, false, false, Table::new())
                          .map(|_| Type::Exchange).ok();
  channel1.close(200, "").unwrap();
  channel2.close(200, "").unwrap();
  opt_queue_type.or(opt_exchange_type)
}

fn parse_bridge_channels(bridge_channels: &str) -> Vec<Binding>{
  let mut bindings: Vec<Binding> = Vec::new();
  let strs: Vec<Vec<&str>> = bridge_channels.split(",").map(|s| s.split(":").collect()).collect();
  for i in 0..strs.len(){
    bindings.push(Binding{pg_channel: strs[i][0].trim().to_string(),
                        amqp_entity: strs[i].get(1).unwrap_or(&"").trim().to_string(),
                        amqp_entity_type: None
                 });
  }
  let mut cleaned_bindings : Vec<Binding> = bindings.into_iter().filter(|x| !x.pg_channel.is_empty() && !x.amqp_entity.is_empty())
                                          .collect();
  if cleaned_bindings.len() == 0 {
    panic!("No bindings(e.g. pgchannel1:queue1) specified in \"{}\"", bridge_channels)
  }
  cleaned_bindings.sort();
  cleaned_bindings.dedup_by(|a, b|{
    let is_dup = a.pg_channel == b.pg_channel;
    if is_dup{
      panic!("Cannot have duplicate PostgreSQL channels.");
    }
    is_dup
  });
  cleaned_bindings
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
    assert!(vec![Binding{pg_channel: "pgchannel1".to_string(), amqp_entity: "exchange1".to_string(), amqp_entity_type: None}]
            == parse_bridge_channels("pgchannel1:exchange1"));
    assert!(vec![
              Binding{pg_channel: "pgchannel1".to_string(), amqp_entity: "exchange1".to_string(), amqp_entity_type: None},
              Binding{pg_channel: "pgchannel2".to_string(), amqp_entity: "exchange2".to_string(), amqp_entity_type: None}
            ] == parse_bridge_channels("pgchannel1:exchange1,pgchannel2:exchange2"));
    assert!(vec![
              Binding{pg_channel: "pgchannel1".to_string(), amqp_entity: "exchange1".to_string(), amqp_entity_type: None},
              Binding{pg_channel: "pgchannel2".to_string(), amqp_entity: "exchange2".to_string(), amqp_entity_type: None},
              Binding{pg_channel: "pgchannel3".to_string(), amqp_entity: "exchange3".to_string(), amqp_entity_type: None}
            ] == parse_bridge_channels(" pgchannel1 : exchange1 , pgchannel2 : exchange2 , pgchannel3 : exchange3, "));
  }

  use std::panic::catch_unwind;

  #[test]
  fn parse_bridge_channels_panics_if_no_pg_channel_and_exchange_specified() {
    assert!(catch_unwind(|| parse_bridge_channels("   ")).is_err());
    assert!(catch_unwind(|| parse_bridge_channels(":")).is_err());
    assert!(catch_unwind(|| parse_bridge_channels("pgchannel1")).is_err());
    assert!(catch_unwind(|| parse_bridge_channels(":exchange1")).is_err());
    assert!(catch_unwind(|| parse_bridge_channels("pgchannel1:")).is_err());
    assert!(catch_unwind(|| parse_bridge_channels("pgchannel1, pgchannel1:, :exchange3,,")).is_err());
  }

  #[test]
  fn parse_bridge_channels_panics_if_duplicate_pg_channel() {
    assert!(catch_unwind(|| parse_bridge_channels("pgchannel1,pgchannel1:exchange2,pgchannel1:exchange3,")).is_err());
    assert!(catch_unwind(|| parse_bridge_channels("pgchannel2, pgchannel2")).is_err());
  }
}
