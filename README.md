# PostgreSQL to AMQP bridge 

Bridge postgresql notifications to existing AMQP entities.

## Interface

```shell
POSTGRESQL_URI="postgres://postgres@localhost" \
AMQP_HOST_PORT="127.0.0.1:5672" \
BRIDGE_CHANNELS="pgchannel1,pgchannel2:direct_exchange,pgchannel3:topic_exchange" \
cargo run
```

The BRIDGE_CHANNELS specify a list of pg notification channels bridged to optional exchanges. 

You can specify a routing key through sql with the format ```routing_key|message```:

## Sending notification to a queue

```sql
NOTIFY pgchannel1, 'task_queue|Task message';
```

Since ```pgchannel1``` has no exchange declared in BRIDGE_CHANNELS "Task message" will be sent to the queue named ```task_queue```.

This interface is similar to the ```basic_publish(exchange='', routing_key='task_queue', "Task message")``` function defined in many amqp libs.

## Sending a notification to a direct exchange

```sql
NOTIFY pgchannel2, 'direct_key|Direct message';
```

Since there is a ```pgchannel2:direct_exchange``` declared in BRIDGE_CHANNELS "Direct message" will be sent to ```direct_exchange``` with a routing key of ```direct_key```.

Again similar to: ```basic_publish(exchange='direct_exchange', routing_key='direct_key', "Direct message")```

## Sending a notification to a topic exchange

```sql
NOTIFY pgchannel3, '*.orange|Topic message';
NOTIFY pgchannel3, 'quick.brown.fox|Topic message';
NOTIFY pgchannel3, 'lazy.#|Topic message';
```

You can specify the routing key with the usual syntax used for topic exchanges.
