FROM debian:jessie

ARG VERSION="0.0.0"

ENV POSTGRESQL_URI="postgres://postgres@postgresql"
ENV AMQP_URI="amqp://rabbitmq//"
ENV BRIDGE_CHANNELS="events:amq.topic"

RUN DEBIAN_FRONTEND="noninteractive" && BUILD_DEPS="curl libssl-dev xz-utils" && \
    apt-get -qq update && \
    apt-get -qq install -y --no-install-recommends $BUILD_DEPS openssl ca-certificates dnsutils && \
    cd /tmp && \
    curl -SLO https://github.com/subzerocloud/pg-amqp-bridge/releases/download/${VERSION}/pg-amqp-bridge-${VERSION}-x86_64-unknown-linux-gnu.tar.gz && \
    tar zxf pg-amqp-bridge-${VERSION}-x86_64-unknown-linux-gnu.tar.gz && \
    mv pg-amqp-bridge /usr/local/bin/pg-amqp-bridge && \
    cd / && \
    apt-get -qq purge --auto-remove -y $BUILD_DEPS && \
    apt-get -qq clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

CMD exec pg-amqp-bridge
