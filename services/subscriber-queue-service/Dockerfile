FROM golang:1.26-alpine AS builder

# Install RocksDB build dependencies
RUN apk add --no-cache \
    build-base \
    rocksdb-dev \
    snappy-dev \
    lz4-dev \
    zstd-dev \
    bzip2-dev \
    zlib-dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o /bin/service ./services/subscriber-queue-service

FROM alpine:3.20
RUN apk add --no-cache ca-certificates rocksdb snappy lz4-libs zstd-libs bzip2 zlib
COPY --from=builder /bin/service /bin/service
ENTRYPOINT ["/bin/service"]
