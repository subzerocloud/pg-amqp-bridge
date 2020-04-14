extern crate rustc_test;
extern crate pg_amqp_bridge as bridge;
extern crate futures;
extern crate tokio_core;
extern crate lapin_futures as lapin;
extern crate postgres;
extern crate r2d2;
extern crate r2d2_postgres;

use r2d2::{Pool};
use r2d2_postgres::{PostgresConnectionManager};
use postgres::{Connection, TlsMode};
use futures::*;
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;
use lapin::client::*;
use lapin::channel::*;
use lapin::types::FieldTable;
use std::env;
use std::thread;
use std::time::Duration;
use rustc_test::*;

//Lapin doesn't support amqp://localhost// format.
const TEST_AMQP_HOST_PORT: &str = "127.0.0.1:5672";
const TEST_AMQP_URI: &str = "amqp://localhost//";
const TEST_PG_URI: &str = "postgres://postgres@localhost";

const TEST_1_PG_CHANNEL: &str = "test_1_pgchannel";
const TEST_1_QUEUE: &str = "test_1_queue";

const TEST_2_PG_CHANNEL: &str = "test_2_pgchannel";
const TEST_2_QUEUE: &str = "test_2_queue";
const TEST_2_EXCHANGE: &str = "test_2_direct_exchange";

const TEST_3_PG_CHANNEL: &str = "test_3_pgchannel";
const TEST_3_QUEUE: &str = "test_3_queue";
const TEST_3_EXCHANGE: &str = "test_3_topic_exchange";

/*
 * Lapin is used for all the tests since rust-amqp has no way to exit a consumer
 * without panicking, this doesn't let the tests do assert! reliably.
 * See issue: https://github.com/Antti/rust-amqp/issues/23
*/

fn publishing_to_queue_works() {
  let mut core = Core::new().unwrap();
  let handle = core.handle();
  let addr = TEST_AMQP_HOST_PORT.parse().unwrap();

  let pg_conn = Connection::connect(TEST_PG_URI, TlsMode::None).unwrap();

  let _ = core.run(
    TcpStream::connect(&addr, &handle)
    .and_then(|stream| Client::connect(stream, &ConnectionOptions::default()) )
    .and_then(|client| client.create_channel())
    .and_then(|channel|
      channel.queue_declare(TEST_1_QUEUE, &QueueDeclareOptions::default(), FieldTable::new())
      .and_then(move |_| 
        channel.basic_consume(TEST_1_QUEUE, "my_consumer_1", &BasicConsumeOptions::default())
        .and_then(move |stream|{
          pg_conn.execute(format!("NOTIFY {}, '{}|Queue test'", TEST_1_PG_CHANNEL, TEST_1_QUEUE).as_str(), &[]).unwrap();
          stream.into_future().map_err(|(err, _)| err)
          .and_then(move |(message, _)| {
            let msg = message.unwrap();
            assert_eq!(msg.data, b"Queue test");
            channel.basic_ack(msg.delivery_tag)
          })
        })
      )
    )
  );
}

fn publishing_to_direct_exchange_works() {
  let mut core = Core::new().unwrap();
  let handle = core.handle();
  let addr = TEST_AMQP_HOST_PORT.parse().unwrap();

  let pg_conn = Connection::connect(TEST_PG_URI, TlsMode::None).unwrap();

  let _ = core.run(
    TcpStream::connect(&addr, &handle)
    .and_then(|stream| Client::connect(stream, &ConnectionOptions::default()) )
    .and_then(|client| client.create_channel())
    .and_then(|channel|{
      let ch1 = channel.clone();
      let ch2 = channel.clone();
      channel.queue_declare(TEST_2_QUEUE, &QueueDeclareOptions::default(), FieldTable::new())
      .and_then(move |_|
        channel.exchange_declare(TEST_2_EXCHANGE, "direct",
                                          &ExchangeDeclareOptions{
                                            passive: false, durable: false,
                                            auto_delete: true, internal: false,
                                            nowait: false,
                                            ..Default::default()
                                          }, FieldTable::new())
        .and_then(move |_| channel.queue_bind(TEST_2_QUEUE, TEST_2_EXCHANGE, "test_direct_key", &QueueBindOptions::default(), FieldTable::new()))
      )
      .and_then(move |_| ch1.basic_consume(TEST_2_QUEUE, "my_consumer_2", &BasicConsumeOptions::default()))
      .and_then(move |stream| {
        let _ = pg_conn.execute(format!("NOTIFY {}, 'test_direct_key|Direct exchange test'", TEST_2_PG_CHANNEL).as_str(), &[]);
        stream.into_future().map_err(|(err, _)| err)
      })
      .and_then(move |(message, _)| {
        let msg = message.unwrap();
        assert_eq!(msg.data, b"Direct exchange test");
        ch2.basic_ack(msg.delivery_tag)
      })
    })
  );
}

fn publishing_to_topic_exchange_works() {
  let mut core = Core::new().unwrap();
  let handle = core.handle();
  let addr = TEST_AMQP_HOST_PORT.parse().unwrap();

  let pg_conn = Connection::connect(TEST_PG_URI, TlsMode::None).unwrap();

  let _ = core.run(
    TcpStream::connect(&addr, &handle)
    .and_then(|stream| Client::connect(stream, &ConnectionOptions::default()) )
    .and_then(|client| client.create_channel())
    .and_then(|channel|{
      let ch1 = channel.clone();
      let ch2 = channel.clone();
      channel.queue_declare(TEST_3_QUEUE, &QueueDeclareOptions::default(), FieldTable::new())
      .and_then(move |_|
        channel.exchange_declare(TEST_3_EXCHANGE, "topic",
                                &ExchangeDeclareOptions{
                                  passive: false, durable: false,
                                  auto_delete: true, internal: false,
                                  nowait: false,
                                  ..Default::default()
                                }, FieldTable::new())
        .and_then(move |_| channel.queue_bind(TEST_3_QUEUE, TEST_3_EXCHANGE, "*.critical", &QueueBindOptions::default(), FieldTable::new()))
      )
      .and_then(move |_| ch1.basic_consume(TEST_3_QUEUE, "my_consumer_3", &BasicConsumeOptions::default()))
      .and_then(move |stream| {
        pg_conn.execute(format!("NOTIFY {}, 'kern.critical|Kernel critical message'", TEST_3_PG_CHANNEL).as_str(), &[]).unwrap();
        stream.into_future().map_err(|(err, _)| err)
      })
      .and_then(move |(message, _)| {
        let msg = message.unwrap();
        assert_eq!(msg.data, b"Kernel critical message");
        ch2.basic_ack(msg.delivery_tag)
      })
    })
  );
}

/*
 * This is to pass validation of the bridge, the queues still need to be
 * redeclared in the tests due to the inability of using an undeclared queue in a channel
 * See this issue: https://github.com/sozu-proxy/lapin/issues/32
*/
fn setup(){
  let mut core = Core::new().unwrap();
  let handle = core.handle();
  let addr = TEST_AMQP_HOST_PORT.parse().unwrap();

  let _ = core.run(
    TcpStream::connect(&addr, &handle)
    .and_then(|stream| Client::connect(stream, &ConnectionOptions::default()) )
    .and_then(|client| client.create_channel())
    .and_then(|channel|
      channel.queue_declare(TEST_1_QUEUE, &QueueDeclareOptions::default(), FieldTable::new())
      .and_then(move |_| 
        channel.exchange_declare(TEST_2_EXCHANGE, "direct", 
                                 &ExchangeDeclareOptions{
                                   passive: false,
                                   durable: false,
                                   auto_delete: true,
                                   internal: false,
                                   nowait: false,
                                   ..Default::default()
                                 }, FieldTable::new())
        .and_then(move |_|
          channel.exchange_declare(TEST_3_EXCHANGE, "topic", 
                                   &ExchangeDeclareOptions{
                                     passive: false,
                                     durable: false,
                                     auto_delete: true,
                                     internal: false,
                                     nowait: false,
                                     ..Default::default()
                                   }, FieldTable::new())
        )
      )
    )
  );
}

fn teardown(){
  let mut core = Core::new().unwrap();
  let handle = core.handle();
  let addr = TEST_AMQP_HOST_PORT.parse().unwrap();

  let _ = core.run(
    TcpStream::connect(&addr, &handle)
    .and_then(|stream| Client::connect(stream, &ConnectionOptions::default()) )
    .and_then(|client| client.create_channel())
    .and_then(|channel|{
      channel.queue_delete(TEST_1_QUEUE, &QueueDeleteOptions::default())
      .and_then(move |_| 
        channel.queue_delete(TEST_2_QUEUE, &QueueDeleteOptions::default())
        .and_then(move |_| 
          channel.queue_delete(TEST_3_QUEUE, &QueueDeleteOptions::default())
        )
      )
    })
  );
}

fn add_test<F: FnOnce() + Send + 'static>(tests: &mut Vec<TestDescAndFn>, name: String, test: F) {
  tests.push(TestDescAndFn {
    desc: TestDesc {
      name: test::DynTestName(name),
      ignore: false,
      should_panic: test::ShouldPanic::No,
      allow_fail: false
    },
    testfn: TestFn::dyn_test_fn(test)
  });
}

fn main() {
  let args: Vec<_> = env::args().collect();
  let mut tests = Vec::new();

  let bridge_channels = format!("{}:{},{}:{},{}:{}", 
                                TEST_1_PG_CHANNEL, TEST_1_QUEUE,
                                TEST_2_PG_CHANNEL, TEST_2_EXCHANGE,
                                TEST_3_PG_CHANNEL, TEST_3_EXCHANGE);

  setup();
  add_test(&mut tests, "publishing_to_queue_works".to_string(), publishing_to_queue_works);
  add_test(&mut tests, "publishing_to_direct_exchange_works".to_string(), publishing_to_direct_exchange_works);
  add_test(&mut tests, "publishing_to_topic_exchange_works".to_string(), publishing_to_topic_exchange_works);

  let pool = Pool::builder()
    .connection_timeout(Duration::from_secs(1))
    .build(PostgresConnectionManager::new(TEST_PG_URI.to_string(), r2d2_postgres::TlsMode::None).unwrap())
    .unwrap();
  thread::spawn(move ||
    bridge::start(pool, &TEST_AMQP_URI, &bridge_channels, &(1 as u8))
  );
  thread::sleep(Duration::from_secs(4));
  test::test_main(&args, tests);
  teardown();
}

