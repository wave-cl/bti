# syntax=docker/dockerfile:1

# Build stage — runs under QEMU for non-native platforms (e.g. arm64 on amd64 CI)
FROM rust:1-alpine AS builder

RUN apk add --no-cache git musl-dev

# squic-rust is a sibling workspace dependency (path = "../../../squic-rust" from crates/bti)
RUN git clone --depth 1 https://github.com/wave-cl/squic-rust.git /squic-rust

WORKDIR /build
COPY . .

RUN cargo build --release --locked
RUN cp target/release/bti /bti

# Minimal user/group (UID/GID 6880)
RUN printf 'bti:x:6880:6880:bti:/:/sbin/nologin\n' > /tmp/passwd && \
    printf 'bti:x:6880:\n' > /tmp/group

# Assemble root-fs in a single layer
FROM alpine AS rootfs
COPY --from=builder /bti        /rootfs/bti
COPY --from=builder /tmp/passwd /rootfs/etc/passwd
COPY --from=builder /tmp/group  /rootfs/etc/group

# Final minimal scratch image
FROM scratch
COPY --from=rootfs /rootfs /

USER bti:bti

VOLUME ["/data"]

ENV BTI_DB_PATH=/data/db \
    LOG_LEVEL=info

EXPOSE 8080
EXPOSE 6880/udp
EXPOSE 6881/udp

ENTRYPOINT ["/bti"]
CMD ["web", "--crawl"]
