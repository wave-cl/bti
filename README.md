# bti

BitTorrent indexer. Crawls the DHT network, fetches torrent metadata, and serves a searchable web UI.

## Architecture

```
[DHT network] → bti crawl → sQUIC sync → bti web → HTTP UI
```

Two deployment modes:
- **Single machine** — crawler and web in one process
- **Split** — crawler on a server, web node(s) sync over sQUIC

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/wave-cl/bti/main/install.sh | sh
```

Installs to `~/.local/bin` (or `/usr/local/bin` if root). Detects OS and architecture automatically.

**Server setup** — installs and starts the crawler as a systemd service:

```sh
curl -fsSL https://raw.githubusercontent.com/wave-cl/bti/main/install.sh | sudo sh -s -- --server
```

## Build from source

```sh
cargo build --release
```

## Usage

### Single machine

```sh
bti web --crawl
```

Opens the web UI at `http://localhost:8080` with the crawler running in-process.

### Split deployment

**Crawler** (on server):

```sh
bti crawl
```

Prints its public key on first run. Note it — web nodes need it to connect.

**Web node** (anywhere):

```sh
bti web --crawler-addr <ip>:6880 --crawler-key <pubkey>
```

Syncs metadata from the crawler over sQUIC and serves the UI at `:8080`.

### Docker

`compose.example.yaml` is a template for running a web node against a remote
crawler. Copy it and fill in the two crawler values:

```sh
cp compose.example.yaml compose.yaml
```

`compose.yaml` itself is gitignored, since it pins one deployment. The address
is the crawler's `host:6880`, and the key is the public key it prints at
startup — the same pair `--crawler-addr` and `--crawler-key` take below. A
wrong key fails the handshake outright rather than degrading, because sQUIC
pins it.

## Options

```
bti crawl
  --migrate <file>      Import from binary dump ("-" for stdin)

bti web
  --crawl               Run crawler in-process
  --crawler-addr <addr> Remote crawler address (requires --crawler-key)
  --crawler-key <key>   Crawler Ed25519 public key (64 hex chars or base58)
  --listen <addr>       HTTP listen address (default: 0.0.0.0:8080)

bti compact             Reclaim disk space in the database
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BTI_DB_PATH` | `~/.bti/db` | Database file path |
| `SYNC_ADDR` | `0.0.0.0:6880` | sQUIC sync listen address |
| `DHT_PORT` | `6881` | DHT UDP listen port |

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 6881 | UDP | DHT network |
| 6880 | UDP/QUIC | sQUIC sync |
| 8080 | TCP | Web UI & API |
