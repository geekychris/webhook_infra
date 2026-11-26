FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/tool ./tools/producer-ui

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/tool /bin/tool
ENTRYPOINT ["/bin/tool"]
