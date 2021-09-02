# PostgreSQL to AMQP bridge 

#### Send messages to RabbitMQ from PostgreSQL


![pg-amqp-bridge](/media/pg-amqp-bridge.gif?raw=true "pg-amqp-bridge")

## But Why? ![why](/media/but-why.gif?raw=true "why?")

This tool enables a decoupled architecture, think sending emails when a user signs up. Instead of having explicit code in your `signup` function that does the work (and slows down your response), you just have to worry about inserting the row into the database. After this, a database trigger (see below) will generate an event which gets sent to RabbitMQ. From there, you can have multiple consumers reacting to that event (send signup email, send sms notification). Those consumers tend to be very short, self contained scripts.
If you pair **pg-amqp-bridge**  and the [Web STOMP](https://www.rabbitmq.com/web-stomp.html) plugin for RabbitMQ , you can enable real time updates with almost zero code.

The larger goal is to enable the development of backends around [PostgREST](https://postgrest.com)/[subZero](https://subzero.cloud/) philosophy. Check out the [PostgREST Starter Kit](https://github.com/subzerocloud/postgrest-starter-kit) to see how `pg-amqp-bridge` fits in a larger project.

## Alternative upstreams, SSL and WAL events support (commercial)
For upstreams other then RabbitMQ (and additional features) check out [pg-event-proxy](https://github.com/subzerocloud/pg-event-proxy-example)

Currently the supported upstreams
- amqp 0.9 (RabbitMQ)
- mqtt (Apache ActiveMQ, Cassandana, HiveMQ, Mosquitto, RabbitMQ, AWS IoT, Amazon MQ, ...)
- redis pubsub (Redis)
- SNS (Amazon Simple Notification Service)
- SQS (Amazon Simple Queue Service)
- Lambda (AWS Lambda)



## Configuration

Configuration is done through environment variables:

- **POSTGRESQL_URI**: e.g. `postgresql://username:password@domain.tld:port/database`
- **AMQP_URI**: e.g. `amqp://rabbitmq//`
- **BRIDGE_CHANNELS**: e.g. `pgchannel1:task_queue,pgchannel2:direct_exchange,pgchannel3:topic_exchange`
- **DELIVERY_MODE**: can be `PERSISTENT` or `NON-PERSISTENT`, default is `NON-PERSISTENT`

**Note:** It's recommended to always use the same name for postgresql channel and exchange/queue in `BRIDGE_CHANNELS`, for example
`app_events:app_events,table_changes:tables_changes`

## Running in console 
#### Install
```shell
VERSION=0.0.1 \
PLATFORM=x86_64-unknown-linux-gnu \
curl -SLO https://github.com/subzerocloud/pg-amqp-bridge/releases/download/${VERSION}/pg-amqp-bridge-${VERSION}-${PLATFORM}.tar.gz && \
tar zxf pg-amqp-bridge-${VERSION}-${PLATFORM}.tar.gz && \
mv pg-amqp-bridge /usr/local/bin
```
#### Run
```shell
POSTGRESQL_URI="postgres://postgres@localhost" \
AMQP_URI="amqp://localhost//" \
BRIDGE_CHANNELS="pgchannel1:task_queue,pgchannel2:direct_exchange,pgchannel3:topic_exchange" \
pg-amqp-bridge
```

## Running as docker container

```shell
docker run --rm -it --net=host \
-e POSTGRESQL_URI="postgres://postgres@localhost" \
-e AMQP_URI="amqp://localhost//" \
-e BRIDGE_CHANNELS="pgchannel1:task_queue,pgchannel2:direct_exchange,pgchannel3:topic_exchange" \
subzerocloud/pg-amqp-bridge
```

You can enable logging of the forwarded messages with the ```RUST_LOG=info``` environment variable.

## Sending messages
**Note**: the bridge doesn't declare exchanges or queues, if they aren't previoulsy declared it will exit with an error.


#### Sending messages to a queue

```sql
NOTIFY pgchannel1, 'Task message';
```

Since ```pgchannel1``` is bound to ```task_queue``` in BRIDGE_CHANNELS ```'Task message'``` will be sent to ```task_queue```.

#### Sending messages to a direct exchange

You can specify a routing key with the format ```routing_key|message```:

```sql
NOTIFY pgchannel2, 'direct_key|Direct message';
```

Since there is a ```pgchannel2:direct_exchange``` declared in BRIDGE_CHANNELS ```'Direct message'``` will be sent to ```direct_exchange``` with a routing key of ```direct_key```.

#### Sending messages to a topic exchange

You can specify the routing key with the usual syntax used for topic exchanges.

```sql
NOTIFY pgchannel3, '*.orange|Topic message';
NOTIFY pgchannel3, 'quick.brown.fox|Topic message';
NOTIFY pgchannel3, 'lazy.#|Topic message';
NOTIFY pgchannel3, 'key|X-First-Header: value1, value2; X-Second-Header: value3|message'
```

## Helper Functions

To make sending messages a bit easier you can setup the following functions in your database

```sql
create schema rabbitmq;

create or replace function rabbitmq.send_message(channel text, routing_key text, message text) returns void as $$
	select	pg_notify(channel, routing_key || '|' || message);
$$ stable language sql;

create or replace function rabbitmq.on_row_change() returns trigger as $$
  declare
    routing_key text;
    row record;
  begin
    routing_key := 'row_change'
                   '.table-'::text || TG_TABLE_NAME::text || 
                   '.event-'::text || TG_OP::text;
    if (TG_OP = 'DELETE') then
        row := old;
    elsif (TG_OP = 'UPDATE') then
        row := new;
    elsif (TG_OP = 'INSERT') then
        row := new;
    end if;
    -- change 'events' to the desired channel/exchange name
    perform rabbitmq.send_message('events', routing_key, row_to_json(row)::text);
    return null;
  end;
$$ stable language plpgsql;
```

After this, you can send events from your stored procedures like this

```sql
rabbitmq.send_message('exchange-name', 'routing-key', 'Hi!');
```

You can stream row changes by attaching a trigger to tables

```sql
create trigger send_change_event
after insert or update or delete on tablename
for each row execute procedure rabbitmq.on_row_change();
```

## Running from source

#### Install Rust

```shell
curl https://sh.rustup.rs -sSf | sh
```

#### Run

```shell
POSTGRESQL_URI="postgres://postgres@localhost" \
AMQP_URI="amqp://localhost//" \
BRIDGE_CHANNELS="pgchannel1:task_queue,pgchannel2:direct_exchange,pgchannel3:topic_exchange" \
cargo run
```

#### Test

**Note**: RabbitMQ and PostgreSQL need to be running on your localhost

```shell
cargo test
```

## Contributing

Anyone and everyone is welcome to contribute.

## Support

* [Slack](https://slack.subzero.cloud/) — Watch announcements, share ideas and feedback
* [GitHub Issues](https://github.com/subzerocloud/pg-amqp-bridge/issues) — Check open issues, send feature requests

## Author

[@steve-chavez](https://github.com/steve-chavez)

## Credits

Inspired by [@FGRibreau](https://github.com/FGRibreau/postgresql-to-amqp)'s work

## License

Copyright © 2017-present subZero Cloud, LLC.<br />
This source code is licensed under [MIT](https://github.com/subzerocloud/pg-amqp-bridge/blob/master/LICENSE.txt) license<br />
The documentation to the project is licensed under the [CC BY-SA 4.0](http://creativecommons.org/licenses/by-sa/4.0/) license.

