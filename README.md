# PostgreSQL to AMQP bridge 

Bridge PostgreSQL notifications to AMQP entities.

## Interface

```shell
POSTGRESQL_URI="postgres://postgres@localhost" \
AMQP_URI="amqp://localhost//" \
BRIDGE_CHANNELS="pgchannel1:task_queue,pgchannel2:direct_exchange,pgchannel3:topic_exchange" \
cargo run
```

The BRIDGE_CHANNELS env var specifies a list of PostgreSQL notifications channels bound to an exchange or queue. 

## Sending notification to a queue

```sql
NOTIFY pgchannel1, 'Task message';
```

Since ```pgchannel1``` is bound to ```task_queue``` in BRIDGE_CHANNELS ```'Task message'``` will be sent to ```task_queue```.

The following message is shown in the console :

```bash
Forwarding "Task message" from pg channel "pgchannel" to Queue "task_queue" with routing key "" 
```

## Sending notification to a direct exchange

You can specify a routing key with the format ```routing_key|message```:

```sql
NOTIFY pgchannel2, 'direct_key|Direct message';
```

Since there is a ```pgchannel2:direct_exchange``` declared in BRIDGE_CHANNELS ```'Direct message'``` will be sent to ```direct_exchange``` with a routing key of ```direct_key```.

This message is shown:

```bash
Forwarding "Direct message" from pg channel "pgchannel2" to Exchange "direct_exchange" with routing key "direct_key" 
```

## Sending notification to a topic exchange

You can specify the routing key with the usual syntax used for topic exchanges.

```sql
NOTIFY pgchannel3, '*.orange|Topic message';
NOTIFY pgchannel3, 'quick.brown.fox|Topic message';
NOTIFY pgchannel3, 'lazy.#|Topic message';
```

**Note**: the bridge doesn't declare exchanges or queues, if they aren't previoulsy declared it will exit with an error.

## Run tests

```shell
cargo test
```
