FROM golang:1.26 AS builder

WORKDIR /src

ENV GOPROXY=https://goproxy.cn,direct \
    GOSUMDB=sum.golang.google.cn

COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/kafkavista ./cmd/kafkavista

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 1000 appuser

WORKDIR /app
COPY --from=builder /out/kafkavista /app/kafkavista

RUN mkdir -p /app/data && chown -R appuser:appuser /app

USER appuser

ENV HTTP_ADDR=:8080 \
    KAFKA_DATABASE_PATH=/app/data/kafkavista.db

EXPOSE 8080
VOLUME ["/app/data"]

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD wget -qO- http://localhost:8080/api/health || exit 1

ENTRYPOINT ["/app/kafkavista"]
