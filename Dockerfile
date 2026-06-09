FROM rust:bookworm AS builder

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends clang cmake pkg-config \
  && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim AS runtime

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/dht-lens /usr/local/bin/dht-lens
COPY scripts/caprover-start.sh /usr/local/bin/caprover-start.sh

RUN chmod +x /usr/local/bin/caprover-start.sh

ENV RUST_LOG=dht_lens=info
ENV DHT_LISTEN_ADDR=0.0.0.0:6881
ENV DHT_LISTEN_ADDR_V6=[::]:6881
ENV DHT_VIRTUAL_NODES=128
ENV PRINT_JSONL=true
ENV STORAGE_ENABLED=true

EXPOSE 6881/udp

CMD ["/usr/local/bin/caprover-start.sh"]
