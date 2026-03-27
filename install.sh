#!/bin/sh
set -e

REPO="wave-cl/bti"
INSTALL_DIR="${BTI_INSTALL_DIR:-}"
SERVER_MODE=false

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --server) SERVER_MODE=true ;;
    esac
done

info() { printf "  \033[1m%s\033[0m\n" "$1"; }
err()  { printf "  \033[31merror:\033[0m %s\n" "$1" >&2; exit 1; }

# Detect OS and architecture
OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
    Linux)  OS_NAME="linux" ;;
    Darwin) OS_NAME="darwin" ;;
    *)      err "unsupported OS: $OS" ;;
esac

case "$ARCH" in
    x86_64|amd64)   TARGET="x86_64-linux-gnu" ;;
    aarch64|arm64)   TARGET="aarch64-linux-gnu" ;;
    *)               err "unsupported architecture: $ARCH" ;;
esac

# Fix target for macOS
if [ "$OS_NAME" = "darwin" ]; then
    case "$ARCH" in
        x86_64|amd64)  TARGET="x86_64-apple-darwin" ;;
        aarch64|arm64) TARGET="aarch64-apple-darwin" ;;
    esac
fi

# Determine install directory
if [ -n "$INSTALL_DIR" ]; then
    BIN_DIR="$INSTALL_DIR"
elif [ "$(id -u)" -eq 0 ]; then
    BIN_DIR="/usr/local/bin"
else
    BIN_DIR="$HOME/.local/bin"
fi

# Detect download tool
if command -v curl >/dev/null 2>&1; then
    fetch() { curl -fsSL "$1"; }
elif command -v wget >/dev/null 2>&1; then
    fetch() { wget -qO- "$1"; }
else
    err "curl or wget required"
fi

# Get latest release tag
info "Fetching latest release..."
LATEST=$(fetch "https://api.github.com/repos/$REPO/releases/latest" | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')
[ -z "$LATEST" ] && err "could not determine latest version"
info "Latest version: $LATEST"

# Download and extract
URL="https://github.com/$REPO/releases/download/$LATEST/bti-${LATEST}-${TARGET}.tar.gz"
info "Downloading bti $LATEST for $TARGET..."

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

fetch "$URL" > "$TMPDIR/bti.tar.gz" || err "download failed — no release for $TARGET?"

info "Installing to $BIN_DIR..."
mkdir -p "$BIN_DIR"
tar -xzf "$TMPDIR/bti.tar.gz" -C "$BIN_DIR"

# Verify
if ! "$BIN_DIR/bti" --version >/dev/null 2>&1; then
    err "installation failed — bti not executable"
fi

VERSION=$("$BIN_DIR/bti" --version 2>&1 || echo "unknown")
info "Installed: $VERSION"

# PATH setup for non-root installs
if [ "$BIN_DIR" = "$HOME/.local/bin" ]; then
    case ":$PATH:" in
        *":$BIN_DIR:"*) ;;  # already in PATH
        *)
            SHELL_NAME=$(basename "$SHELL" 2>/dev/null || echo "unknown")
            case "$SHELL_NAME" in
                bash)
                    RC="$HOME/.bashrc"
                    echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$RC"
                    info "Added ~/.local/bin to PATH in $RC"
                    ;;
                zsh)
                    RC="$HOME/.zshrc"
                    echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$RC"
                    info "Added ~/.local/bin to PATH in $RC"
                    ;;
                fish)
                    RC="$HOME/.config/fish/config.fish"
                    mkdir -p "$(dirname "$RC")"
                    echo 'fish_add_path ~/.local/bin' >> "$RC"
                    info "Added ~/.local/bin to PATH in $RC"
                    ;;
                *)
                    info "Add $BIN_DIR to your PATH"
                    ;;
            esac
            info "Restart your shell or run: export PATH=\"$BIN_DIR:\$PATH\""
            ;;
    esac
fi

# Server setup
if [ "$SERVER_MODE" = true ]; then
    info "Setting up bti crawler server..."

    # Must be root
    if [ "$(id -u)" -ne 0 ]; then
        err "--server requires root"
    fi

    # Must be Linux
    if [ "$OS_NAME" != "linux" ]; then
        err "--server is only supported on Linux"
    fi

    # Create config directory
    mkdir -p /etc/bti
    chmod 755 /etc/bti

    # Create data directory
    mkdir -p /var/lib/bti
    chmod 755 /var/lib/bti

    # Install systemd service (always overwrite for upgrades)
    if command -v systemctl >/dev/null 2>&1; then
        info "Installing systemd service..."
        cat > /etc/systemd/system/bti-crawler.service << 'SVC'
[Unit]
Description=BTI torrent metadata crawler
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/bti crawl
Restart=on-failure
RestartSec=5
StateDirectory=bti
Environment=BTI_DB_PATH=/var/lib/bti/db
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
SVC
        chmod 644 /etc/systemd/system/bti-crawler.service
        systemctl daemon-reload
        systemctl enable bti-crawler

        if systemctl is-active bti-crawler >/dev/null 2>&1; then
            info "Restarting bti-crawler..."
            systemctl restart bti-crawler
        else
            info "Starting bti-crawler..."
            systemctl start bti-crawler
        fi

        # Verify
        sleep 2
        if systemctl is-active bti-crawler >/dev/null 2>&1; then
            info "bti-crawler is running"
        else
            err "bti-crawler failed to start — check: journalctl -u bti-crawler"
        fi
    else
        info "systemd not found — skipping service installation"
        info "Start manually: BTI_DB_PATH=/var/lib/bti/db bti crawl"
    fi

    printf "\n"
    info "Crawler is running on port 6881 (DHT) and 6880 (sQUIC sync)"
    info "Database: /var/lib/bti/db"
    printf "\n"
    info "Useful commands:"
    printf "  Status:   systemctl status bti-crawler\n"
    printf "  Logs:     journalctl -u bti-crawler -f\n"
    printf "  Stop:     systemctl stop bti-crawler\n"
    printf "  Restart:  systemctl restart bti-crawler\n"
    printf "\n"
else
    printf "\n"
    info "Usage:"
    printf "  Start crawler:           bti crawl\n"
    printf "  Start web + crawler:     bti web --crawl\n"
    printf "  Sync from remote:        bti sync --crawler-addr <host>:6880 --crawler-key <pubkey>\n"
    printf "\n"
    info "Server setup:"
    printf "  Install as service:  curl -fsSL https://raw.githubusercontent.com/wave-cl/bti/main/install.sh | sudo sh -s -- --server\n"
    printf "\n"
fi
