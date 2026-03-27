# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.6 AS xx

FROM --platform=$BUILDPLATFORM rust:1-alpine AS builder

COPY --from=xx /usr/bin/xx-* /usr/bin/
ARG TARGETPLATFORM

# Build dependencies
RUN apk add --no-cache git musl-dev
RUN xx-apk add --no-cache musl-dev gcc

# squic-rust is a sibling workspace dependency (path = "../../../squic-rust" from crates/bti)
RUN git clone --depth 1 https://github.com/wave-cl/squic-rust.git /squic-rust

WORKDIR /build
COPY . .

# Add Rust target for cross-compilation and build
RUN rustup target add "$(xx-info rust-target)"
RUN xx-cargo build --release --locked
RUN cp "target/$(xx-info rust-target)/release/bti" /bti && xx-verify /bti

# Minimal user/group files (UID/GID 6880 matches the sQUIC port — memorable)
RUN printf 'bti:x:6880:6880:bti:/:/sbin/nologin\n' > /tmp/passwd && \
    printf 'bti:x:6880:\n' > /tmp/group

# Assemble root-fs in a single layer before copying to scratch
FROM --platform=$BUILDPLATFORM alpine AS rootfs
COPY --from=builder /bti       /rootfs/bti
COPY --from=builder /tmp/passwd /rootfs/etc/passwd
COPY --from=builder /tmp/group  /rootfs/etc/group

# Final minimal image
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
