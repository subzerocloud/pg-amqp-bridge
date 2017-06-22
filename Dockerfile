FROM debian:jessie

ENV VERSION="0.0.1"
ENV POSTGRESQL_URI="postgres://postgres@postgresql"
ENV AMQP_URI="amqp://rabbitmq//"
ENV BRIDGE_CHANNELS="events:amq.topic"

RUN BUILD_DEPS="curl ca-certificates xz-utils" && \
    apt-get -qq update && \
    apt-get -qq install -y --no-install-recommends $BUILD_DEPS && \
    cd /tmp && \
	curl -SLO https://github.com/subzerocloud/pg-amqp-bridge/releases/download/v${VERSION}/pg-amqp-bridge-v${VERSION}-linux.gz && \
    gunzip pg-amqp-bridge-v${VERSION}-linux.gz && \
    mv pg-amqp-bridge /usr/local/bin/pg-amqp-bridge && \
    cd / && \
    apt-get -qq purge --auto-remove -y $BUILD_DEPS && \
    apt-get -qq clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

CMD pg-amqp-bridge