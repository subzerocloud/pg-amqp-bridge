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

#[derive(Debug)]
struct Bridge{
  pg_channel: String,
  amqp_entity: String,
}

#[derive(Debug)]
struct Envelope{
  pg_channel: String,
  routing_key: RoutingKey,
  message: Message,
  amqp_entity: String,
}

type RoutingKey = String;
type Message = String;

const SEPARATOR: &str = "|";

pub fn start_bridge(amqp_host_port: &String, pg_uri: &String, bridge_channels: &String){
  let amqp_addr: SocketAddr = amqp_host_port.parse().expect("amqp_host_port should be in format '127.0.0.1:5672'");
  let bridges = parse_bridges(bridge_channels);

  let (sender, receiver): (Sender<Envelope>, Receiver<Envelope>) = channel();

  for bridge in bridges{
    spawn_pg_listener(pg_uri.clone(), bridge, sender.clone());
  }

  amqp_publisher(amqp_addr, receiver);
}

fn parse_bridges(bridge_channels: &str) -> Vec<Bridge>{
  let mut bridges: Vec<Bridge> = Vec::new();
  let str_bridges: Vec<Vec<&str>> = bridge_channels.split(",").map(|s| s.split(":").collect()).collect();
  for i in 0..str_bridges.len(){
    bridges.push(Bridge{pg_channel: str_bridges[i][0].to_string(),
                        amqp_entity: if str_bridges[i].len() > 1 {str_bridges[i][1].to_string()} else {String::new()}
                 });
  }
  bridges
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
          let (routing_key, message) = parse_payload(&notification.payload);
          let envelope = Envelope{pg_channel: bridge.pg_channel.clone(),
                                  routing_key: routing_key,
                                  message: message,
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

fn parse_payload(payload: &String) -> (RoutingKey, Message){
  let v: Vec<&str> = payload.split(SEPARATOR).collect();
  if v.len() > 1 {
    (v[0].to_string(), v[1].to_string())
  } else {
    (String::new(), v[0].to_string())
  }
}

fn amqp_publisher(amqp_addr: SocketAddr, receiver: Receiver<Envelope>){
  let mut core = Core::new().expect("Could not create event loop");
  let handle = core.handle();
  let _ = core.run(
    TcpStream::connect(&amqp_addr, &handle)
    .and_then(|stream| Client::connect(stream, &ConnectionOptions::default()))
    .and_then(|client| client.create_confirm_channel())
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
  );
}
