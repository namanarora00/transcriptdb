#!/usr/bin/env bash
# Unit and end-to-end tests (synthetic data only).
set -euo pipefail
cd "$(dirname "$0")/.."
# Homebrew's rustup is keg-only, so cargo may not be on PATH
command -v cargo >/dev/null || export PATH="/opt/homebrew/opt/rustup/bin:$HOME/.cargo/bin:$PATH"
cargo test --release
