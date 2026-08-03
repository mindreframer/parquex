#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"

compose_used=0
rustfs_init_marker="${project_root}/_build/qa/rustfs_initialized_container_id"

qa_stage() {
  printf '\n[qa/%s] %s\n' "$1" "$2"
}

run_quiet() {
  local output
  local status

  if output="$("$@" 2>&1)"; then
    return 0
  else
    status=$?
    printf '%s\n' "${output}" >&2
    return "${status}"
  fi
}

cleanup() {
  status=$?
  trap - EXIT

  if [[ "${status}" -ne 0 && "${compose_used}" -eq 1 ]]; then
    docker compose ps --all || true
    docker compose logs --no-color --tail=200 rustfs rustfs-init || true
  fi

  exit "${status}"
}

trap cleanup EXIT

qa_stage elixir "dependencies, format, compile"
run_quiet env MIX_ENV=test mix deps.get --check-locked
mix format --check-formatted
MIX_ENV=test mix compile --warnings-as-errors

qa_stage docker "start or reuse RustFS"
run_quiet docker info
docker compose config --quiet
compose_used=1
previous_rustfs_container_id="$(docker compose ps --quiet rustfs)"
run_quiet docker compose up --detach --wait --wait-timeout 45 rustfs
rustfs_container_id="$(docker compose ps --quiet rustfs)"
test -n "${rustfs_container_id}"

if [[ "${previous_rustfs_container_id}" == "${rustfs_container_id}" ]]; then
  rustfs_state="reused"
else
  rustfs_state="started"
fi

initialized_container_id=""
if [[ -f "${rustfs_init_marker}" ]]; then
  initialized_container_id="$(< "${rustfs_init_marker}")"
fi

if [[ "${initialized_container_id}" != "${rustfs_container_id}" ]]; then
  run_quiet docker compose run --rm rustfs-init
  mkdir -p "$(dirname "${rustfs_init_marker}")"
  printf '%s\n' "${rustfs_container_id}" >"${rustfs_init_marker}"
fi

printf '[qa/docker] RustFS ready (%s)\n' "${rustfs_state}"

export PARQUEX_RUSTFS_INTEGRATION=1
export AWS_ACCESS_KEY_ID=parquex-test-access
export AWS_SECRET_ACCESS_KEY=parquex-test-secret-not-for-production
export AWS_REGION=us-east-1

qa_stage elixir "ExUnit"
MIX_ENV=test mix test --no-compile

qa_stage rust "format"
cargo +1.91.0 fmt --manifest-path native/parquex_nif/Cargo.toml --all -- --check

qa_stage rust "check"
cargo +1.91.0 check --manifest-path native/parquex_nif/Cargo.toml --locked --all-targets

qa_stage rust "clippy"
cargo +1.91.0 clippy --manifest-path native/parquex_nif/Cargo.toml --locked --all-targets -- -D warnings

qa_stage rust "tests"
cargo +1.91.0 test --manifest-path native/parquex_nif/Cargo.toml --locked --all-targets

qa_stage ok "all checks passed"
