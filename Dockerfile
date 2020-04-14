FROM rust:slim AS builder

WORKDIR /usr/src/pg-amqp-bridge
COPY . .
RUN cargo install --path .


FROM debian:buster-slim

COPY --from=builder /usr/local/cargo/bin/pg-amqp-bridge /usr/local/bin/pg-amqp-bridge

CMD ["pg-amqp-bridge"]
