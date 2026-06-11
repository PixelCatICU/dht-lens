FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl tar \
  && rm -rf /var/lib/apt/lists/*

ARG DHT_LENS_RELEASE_TAG=latest
ARG CAPROVER_GIT_COMMIT_SHA=local

RUN echo "release=${DHT_LENS_RELEASE_TAG} commit=${CAPROVER_GIT_COMMIT_SHA}" \
  && curl --fail --location --retry 5 --connect-timeout 20 \
    -o /tmp/dht-lens-linux-amd64 \
    "https://github.com/PixelCatICU/dht-lens/releases/download/${DHT_LENS_RELEASE_TAG}/dht-lens-linux-amd64" \
  && curl --fail --location --retry 5 --connect-timeout 20 \
    -o /tmp/SHA256SUMS \
    "https://github.com/PixelCatICU/dht-lens/releases/download/${DHT_LENS_RELEASE_TAG}/SHA256SUMS" \
  && cd /tmp \
  && sha256sum -c SHA256SUMS \
  && mv /tmp/dht-lens-linux-amd64 /usr/local/bin/dht-lens \
  && chmod +x /usr/local/bin/dht-lens \
  && rm -f /tmp/SHA256SUMS

COPY entrypoint.sh /usr/local/bin/dht-lens-entrypoint.sh

RUN chmod +x /usr/local/bin/dht-lens-entrypoint.sh

ENV RUST_LOG=dht_lens=info
ENV DHT_LISTEN_ADDR=0.0.0.0:6881
ENV DHT_ROUTING_TABLE_MAX_NODES=5000
ENV INFO_HASH_QUEUE_SIZE=1000
ENV METADATA_MAX_CONCURRENT_FETCHES=16
ENV METADATA_TIMEOUT_SECS=15
ENV PRINT_JSONL=false
ENV STORAGE_ENABLED=true

EXPOSE 6881/udp

CMD ["/usr/local/bin/dht-lens-entrypoint.sh"]
