# PostgreSQL to AMQP bridge 

Bridge postgresql notifications to existing AMQP entities.

## Example

Run it: 

```shell
POSTGRESQL_URI="postgres://postgres@localhost" \
AMQP_HOST_PORT="127.0.0.1:5672" \
BRIDGE_CHANNELS="pgchannel1:my_queue1,pgchannel2:my_queue2" \
cargo run
```

If the AMQP entities do not exist, the program will panic.

Then in psql:

```sql
NOTIFY pgchannel1, 'First message';
NOTIFY pgchannel2, 'Second message';
```

The following messages will be printed to stdout:

```
Forwarding "First message" from pg channel "pgchannel1" to amqp entity "my_queue1"
Forwarding "Second message" from pg channel "pgchannel2" to amqp entity "my_queue2"
```
