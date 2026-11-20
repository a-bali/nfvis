# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM node:24-bookworm-slim AS web
WORKDIR /src/web
COPY web/package*.json ./
RUN npm ci
COPY web/ ./
COPY internal/webui/ /src/internal/webui/
RUN npm run build

FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=web /src/internal/webui/dist ./internal/webui/dist
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/nfvis ./cmd/nfvis

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata && addgroup -S -g 10001 nfvis && adduser -S -u 10001 -G nfvis nfvis && mkdir -p /data && chown nfvis:nfvis /data
COPY --from=build /out/nfvis /usr/local/bin/nfvis
USER nfvis
WORKDIR /data
EXPOSE 2055/udp 8080/tcp
ENTRYPOINT ["nfvis"]
CMD ["--config", "/data/config.yaml"]
