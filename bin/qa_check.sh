#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"

compose_started=0

cleanup() {
  status=$?
  trap - EXIT

  if [[ "${compose_started}" -eq 1 ]]; then
    if [[ "${status}" -ne 0 ]]; then
      docker compose ps --all || true
      docker compose logs --no-color --tail=200 rustfs rustfs-init || true
    fi
    docker compose down --volumes --remove-orphans || true
  fi

  exit "${status}"
}

trap cleanup EXIT

mix deps.get --check-locked
mix format --check-formatted
MIX_ENV=test mix compile --force --warnings-as-errors

docker info >/dev/null
docker compose config --quiet
docker compose down --volumes --remove-orphans
compose_started=1
docker compose up --detach --wait --wait-timeout 45 rustfs
docker compose run --rm rustfs-init

export PARQUEX_RUSTFS_INTEGRATION=1
export AWS_ACCESS_KEY_ID=parquex-test-access
export AWS_SECRET_ACCESS_KEY=parquex-test-secret-not-for-production
export AWS_REGION=us-east-1

MIX_ENV=test mix test

cargo +1.91.0 fmt --manifest-path native/parquex_nif/Cargo.toml --all -- --check
cargo +1.91.0 check --manifest-path native/parquex_nif/Cargo.toml --locked --all-targets
cargo +1.91.0 clippy --manifest-path native/parquex_nif/Cargo.toml --locked --all-targets -- -D warnings
cargo +1.91.0 test --manifest-path native/parquex_nif/Cargo.toml --locked --all-targets
