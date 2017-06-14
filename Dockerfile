FROM scorpil/rust:1.18

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends libssl-dev && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /rust/app
WORKDIR /rust/app

ONBUILD COPY . /rust/app
ONBUILD RUN cargo build --release

CMD cargo run --release
