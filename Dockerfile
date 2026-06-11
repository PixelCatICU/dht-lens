FROM node:26-bookworm-slim

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev

COPY js ./js
COPY scripts/caprover-start.sh /usr/local/bin/caprover-start.sh

RUN chmod +x /usr/local/bin/caprover-start.sh

ENV DHT_LISTEN_ADDR=0.0.0.0:6881
ENV DHT_ROUTING_TABLE_MAX_NODES=200000
ENV METADATA_MAX_CONCURRENT_FETCHES=512
ENV PRINT_JSONL=true
ENV STORAGE_ENABLED=true

EXPOSE 6881/udp

CMD ["/usr/local/bin/caprover-start.sh"]
