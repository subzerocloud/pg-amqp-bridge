# PostgreSQL to AMQP bridge 

Send messages to RabbitMQ from PostgreSQL

![pg-amqp-bridge](/pg-amqp-bridge.gif?raw=true "pg-amqp-bridge")

## Running in console 
#### Install
```shell
VERSION=0.0.1 && \
PLATFORM=x86_64-unknown-linux-gnu && \
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
docker run -d \
-e POSTGRESQL_URI="postgres://postgres@database" \
-e AMQP_URI="amqp://rabbitmq//" \
-e BRIDGE_CHANNELS="pgchannel1:task_queue,pgchannel2:direct_exchange,pgchannel3:topic_exchange" \
--add-host database:DB_IP_ADDRESS \
--add-host rabbitmq:RABBITMQ_IP_ADDRESS \
subzerocloud/pg-amqp-bridge
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

## Test

**Note**: RabbitMQ and PostgreSQL need to be running on your localhost

```shell
cargo test
```

## Sending messages
**Note**: the bridge doesn't declare exchanges or queues, if they aren't previoulsy declared it will exit with an error.


#### Sending messages to a queue

```sql
NOTIFY pgchannel1, 'Task message';
```

Since ```pgchannel1``` is bound to ```task_queue``` in BRIDGE_CHANNELS ```'Task message'``` will be sent to ```task_queue```.

The following message is shown in the console :

```bash
Forwarding "Task message" from pg channel "pgchannel" to Queue "task_queue" with routing key "" 
```

#### Sending messages to a direct exchange

You can specify a routing key with the format ```routing_key|message```:

```sql
NOTIFY pgchannel2, 'direct_key|Direct message';
```

Since there is a ```pgchannel2:direct_exchange``` declared in BRIDGE_CHANNELS ```'Direct message'``` will be sent to ```direct_exchange``` with a routing key of ```direct_key```.

This message is shown:

```bash
Forwarding "Direct message" from pg channel "pgchannel2" to Exchange "direct_exchange" with routing key "direct_key" 
```

#### Sending messages to a topic exchange

You can specify the routing key with the usual syntax used for topic exchanges.

```sql
NOTIFY pgchannel3, '*.orange|Topic message';
NOTIFY pgchannel3, 'quick.brown.fox|Topic message';
NOTIFY pgchannel3, 'lazy.#|Topic message';
```


## Helper Functions

To make sending messages a bit easier you can setup the following functions in your database

**Note**: although in the documentation we use `pgchannel2:direct_exchange` to be clear which is which, we recomment that the name of the channel always matches the exchange/queue name.

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

## Author

Steve Chavez

## License

Copyright © 2017-present subZero Cloud, LLC.<br />
This source code is licensed under [MIT](https://github.com/subzerocloud/pg-amqp-bridge/blob/master/LICENSE.txt) license<br />
The documentation to the project is licensed under the [CC BY-SA 4.0](http://creativecommons.org/licenses/by-sa/4.0/) license.

[![Analytics](https://ga-beacon.appspot.com/UA-79996734-2/pg-amqp-bridge/readme?pixel&useReferer)](https://github.com/igrigorik/ga-beacon)

