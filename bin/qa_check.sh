#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"

mix deps.get --check-locked
mix format --check-formatted
MIX_ENV=test mix compile --force --warnings-as-errors
MIX_ENV=test mix test

cargo +1.91.0 fmt --manifest-path native/parquex_nif/Cargo.toml --all -- --check
cargo +1.91.0 check --manifest-path native/parquex_nif/Cargo.toml --locked --all-targets
cargo +1.91.0 clippy --manifest-path native/parquex_nif/Cargo.toml --locked --all-targets -- -D warnings
cargo +1.91.0 test --manifest-path native/parquex_nif/Cargo.toml --locked --all-targets
