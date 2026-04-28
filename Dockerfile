FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum main.go ./
RUN go mod tidy && go build -o relay-server main.go

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/relay-server .
EXPOSE 8443
ENTRYPOINT ["./relay-server"]