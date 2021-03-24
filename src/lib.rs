extern crate amqp;
extern crate fallible_iterator;
extern crate r2d2;
extern crate r2d2_postgres;
extern crate postgres;
#[macro_use] extern crate log;
#[macro_use] extern crate maplit;


use amqp::{Session, Basic, protocol, Channel, Table, AMQPError, TableEntry};
use fallible_iterator::FallibleIterator;
use r2d2::{Pool, PooledConnection};
use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls};
use std::default::Default;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
enum Type {
  Exchange,
  Queue
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct Binding{
  pg_channel: String,
  amqp_entity: String
}

//Used for channel ids
struct ChannelCounter{
  counter: u16
}
impl ChannelCounter {
  pub fn new() -> ChannelCounter {
    ChannelCounter { counter: 0 }
  }
  pub fn inc(&mut self) -> u16 {
    self.counter += 1;
    self.counter
  }
}

const SEPARATOR: char = '|';
const HEADERS_SEPARATOR: char = ';';
const HEADER_NAME_VALUE_SEPARATOR: char = ':';
const HEADER_VALUES_SEPARATOR: char = ',';

pub fn start(pool: Pool<PostgresConnectionManager<NoTls>>, amqp_uri: &str, bridge_channels: &str, delivery_mode: &u8){
  let mut children = Vec::new();

  for binding in parse_bridge_channels(bridge_channels){
    children.push(
      spawn_listener_publisher(pool.get().unwrap(), amqp_uri.to_string(), binding, delivery_mode.to_owned()))
  }

  for child in children{
    let _ = child.join();
  }
}

fn spawn_listener_publisher(mut pg_conn: PooledConnection<PostgresConnectionManager<NoTls>>,
                            amqp_uri: String, binding: Binding, delivery_mode: u8) -> JoinHandle<()> {
  thread::spawn(move ||{

    let mut channel_counter = ChannelCounter::new();
    let mut session = wait_for_amqp_session(&amqp_uri, binding.pg_channel.as_str());
    let amqp_entity_type = match get_amq_entity_type(
      &mut session.open_channel(channel_counter.inc()).unwrap(),
      &mut session.open_channel(channel_counter.inc()).unwrap(),
      &binding.amqp_entity){
        None      => {
          error!("The amqp entity {:?} doesn't exist", binding.amqp_entity);
          std::process::exit(1);
        }
        Some(typ) => typ
    };
    let mut local_channel = session.open_channel(channel_counter.inc()).unwrap();

    let listen_command = format!("LISTEN {}", binding.pg_channel);
    pg_conn.execute(listen_command.as_str(), &[]).unwrap();

    println!("Listening on {}...", binding.pg_channel);

    let mut notifications = pg_conn.notifications();
    let mut it = notifications.blocking_iter();

    while let Ok(Some(notification)) = it.next() {
      let (routing_key, message, headers) = parse_notification(&notification.payload());
      let (exchange, key) =
        if amqp_entity_type == Type::Exchange {
          (binding.amqp_entity.as_str(), routing_key)
        } else {
          ("", binding.amqp_entity.as_str())
        };

      let mut publication = local_channel.basic_publish(
          exchange, key, true, false,
          protocol::basic::BasicProperties{
            content_type: Some("text".to_string()),
            headers,
            delivery_mode: Some(delivery_mode),
            ..Default::default()
          },
          message.as_bytes().to_vec());

      // When RMQ connection is lost retry it
      if let Err(e@AMQPError::IoError(_)) = publication {
        error!("{:?}", e);
        session = wait_for_amqp_session(amqp_uri.as_str(), binding.pg_channel.as_str());
        local_channel = match get_amq_entity_type(
          &mut session.open_channel(channel_counter.inc()).unwrap(),
          &mut session.open_channel(channel_counter.inc()).unwrap(),
          &binding.amqp_entity){
            None      => {
              error!("The amqp entity {:?} doesn't exist", binding.amqp_entity);
              std::process::exit(1);
            },
            Some(_) => session.open_channel(channel_counter.inc()).unwrap()
        };
        // Republish message
        publication =
          local_channel.basic_publish(
            exchange, key, true, false,
            protocol::basic::BasicProperties{ content_type: Some("text".to_string()), delivery_mode: Some(delivery_mode), ..Default::default()},
            message.as_bytes().to_vec());
      }

      match publication{
        Ok(_) => {
          info!("{:?} -> {:?} {:?} ( routing_key: {:?}, message: {:?} )",
                binding.pg_channel, amqp_entity_type, binding.amqp_entity, routing_key, message);
        },
        Err(e)  => error!("{:?}", e)
      }
    }

    local_channel.close(200, "").unwrap();
    session.close(200, "");
  })
}


/*
 * Finds the amqp entity type(Queue or Exchange) using two channels because currently rust-amqp hangs up when
 * doing exchange_declare and queue_declare on the same channel.
 * It does this with amqp "passive" set to true.
*/
fn get_amq_entity_type(queue_channel: &mut Channel, exchange_channel: &mut Channel, amqp_entity: &str) -> Option<Type>{
  let opt_queue_type = queue_channel.queue_declare(amqp_entity.clone(), true, false, false, false, false, Table::new())
                       .map(|_| Type::Queue).ok();
  let opt_exchange_type = exchange_channel.exchange_declare(amqp_entity, "", true, false, false, false, false, Table::new())
                          .map(|_| Type::Exchange).ok();
  queue_channel.close(200, "").unwrap();
  //Somehow the exchange channel is not being closed, a solution could be to close session and reopen
  //However when doing that some error messages(they don't seem to affect the bridge) are shown and that could be confusing for the user
  exchange_channel.close(200, "").unwrap();
  opt_exchange_type.or(opt_queue_type)
}

fn parse_bridge_channels(bridge_channels: &str) -> Vec<Binding>{
  let mut bindings: Vec<Binding> = Vec::new();
  let strs: Vec<Vec<&str>> = bridge_channels.split(",").map(|s| s.split(":").collect()).collect();
  for i in 0..strs.len(){
    bindings.push(Binding{pg_channel: strs[i][0].trim().to_string(),
                        amqp_entity: strs[i].get(1).unwrap_or(&"").trim().to_string()});
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

fn parse_notification(payload: &str) -> (&str, &str, Option<Table>){
  let v: Vec<&str> = payload.splitn(3, SEPARATOR).map(|x| x.trim()).collect();
  match v.len() {
    3 => {
        let components: Vec<&str> = v[1].split(HEADERS_SEPARATOR).map(|x| x.trim()).collect();
        let mut headers = Table::new();
        for c in components {
            let array: Vec<&str> = c.splitn(2, HEADER_NAME_VALUE_SEPARATOR).map(|x| x.trim()).collect();
            if let [name, values] = array[..] {
                let values: Vec<&str> = values.split( HEADER_VALUES_SEPARATOR).map(|x| x.trim()).collect();
                let mut fields = vec![];
                for v in values {
                    fields.push(TableEntry::LongString(v.to_owned()));
                }
                headers.insert(name.to_owned(), TableEntry::FieldArray(fields));
            }
        }
        (v[0], v[2], Some(headers))
    },
    2 => (v[0], v[1], None),
    _ => ("", v[0], None)
  }
}

pub fn wait_for_amqp_session(amqp_uri: &str, pg_channel: &str) -> Session {
  println!("Attempting to obtain connection on AMQP server for {} channel..", pg_channel);
  let mut s = Session::open_url(amqp_uri);
  let mut i = 1;
  while let Err(e)  = s {
    println!("{:?}", e);
    let time = Duration::from_secs(i);
    println!("Retrying the AMQP connection for {} channel in {:?} seconds..", pg_channel, time.as_secs());
    thread::sleep(time);
    s = Session::open_url(amqp_uri);
    i *= 2;
    if i > 32 { i = 1 };
  };
  println!("Connection to AMQP server for {} channel successful", pg_channel);
  s.unwrap()
}

#[cfg(test)]
mod tests {
  use super::*;
  #[test]
  fn parse_notification_works() {
    assert!(("my_key", "A message", None) == parse_notification("my_key|A message"));
    assert!(("my_key", "A message", None) == parse_notification("  my_key  |  A message  "));
    //assert!(("my_key", "A message|Rest of message", None) == parse_notification("my_key|A message|Rest of message"));
    assert!(("", "my_key##A message", None) == parse_notification("my_key##A message"));
    assert!(("", "A message", None) == parse_notification("A message"));
    assert!(("", "", None) == parse_notification(""));
    assert!(("mý_kéý", "A mésságé", None) == parse_notification("mý_kéý|A mésságé"));
    assert_eq!(("my_key", "A message", Some(hashmap!{
      "Content-Type".to_owned() => TableEntry::FieldArray(vec![
        TableEntry::LongString("application/json".to_owned()),
        TableEntry::LongString("application/octet-stream".to_owned()),
      ]),
      "X-My-Header".to_owned() => TableEntry::FieldArray(vec![
        TableEntry::LongString("my-value".to_owned()),
      ])
    })), parse_notification("my_key|Content-Type: application/json, application/octet-stream; X-My-Header: my-value|A message"));
  }

  #[test]
  fn parse_bridge_channels_works() {
    assert!(vec![Binding{pg_channel: "pgchannel1".to_string(), amqp_entity: "exchange1".to_string()}]
            == parse_bridge_channels("pgchannel1:exchange1"));
    assert!(vec![
              Binding{pg_channel: "pgchannel1".to_string(), amqp_entity: "exchange1".to_string()},
              Binding{pg_channel: "pgchannel2".to_string(), amqp_entity: "exchange2".to_string()}
            ] == parse_bridge_channels("pgchannel1:exchange1,pgchannel2:exchange2"));
    assert!(vec![
              Binding{pg_channel: "pgchannel1".to_string(), amqp_entity: "exchange1".to_string()},
              Binding{pg_channel: "pgchannel2".to_string(), amqp_entity: "exchange2".to_string()},
              Binding{pg_channel: "pgchannel3".to_string(), amqp_entity: "exchange3".to_string()}
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
