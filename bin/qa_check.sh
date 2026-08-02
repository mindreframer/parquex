#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${project_root}"

compose_started=0
package_audit_dir=""

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

  if [[ -n "${package_audit_dir}" && -d "${package_audit_dir}" ]]; then
    rm -rf -- "${package_audit_dir}"
  fi

  exit "${status}"
}

trap cleanup EXIT

mix deps.get --check-locked
mix format --check-formatted
MIX_ENV=test mix compile --force --warnings-as-errors
MIX_ENV=dev mix docs --warnings-as-errors

package_audit_dir="$(mktemp -d)"
mix hex.build --unpack --output "${package_audit_dir}/package"
test -f "${package_audit_dir}/package/native/parquex_nif/Cargo.lock"
test -f "${package_audit_dir}/package/docs/release.md"
test ! -e "${package_audit_dir}/package/priv/native/parquex_nif.so"
! rg -n 'parquex-test-secret-not-for-production|row-value-that-must-not-enter-telemetry' \
  "${package_audit_dir}/package"
(
  cd "${package_audit_dir}/package"
  MIX_ENV=prod mix deps.get --only prod
  MIX_ENV=prod mix compile --force --warnings-as-errors
)

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
