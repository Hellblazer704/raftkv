FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/raftkvd ./cmd/raftkvd \
 && CGO_ENABLED=0 go build -o /out/raftkv-cli ./cmd/raftkv-cli

FROM alpine:3.20
COPY --from=build /out/raftkvd /out/raftkv-cli /usr/local/bin/
VOLUME /data
ENTRYPOINT ["raftkvd"]
