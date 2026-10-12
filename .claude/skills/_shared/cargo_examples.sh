#!/usr/bin/env bash
# Source from a harness after changing to the repository root. Cargo reports the exact executable
# it built, including target-dir/build.target configuration and environment overrides. Never infer
# an artifact path from a workspace-local target/debug directory, which may hold an older binary.

declare -A CARGO_EXAMPLE_BINARIES=()

cargo_build_examples() {
  local artifacts resolved status example executable
  local -a arguments=()
  [ "$#" -gt 0 ] || { echo "cargo_build_examples needs example names" >&2; return 2; }
  CARGO_EXAMPLE_BINARIES=()
  for example in "$@"; do arguments+=(--example "$example"); done
  artifacts=$(mktemp) || return 1
  if cargo build --quiet --message-format=json-render-diagnostics "${arguments[@]}" >"$artifacts"; then
    :
  else
    status=$?
    rm -f "$artifacts"
    return "$status"
  fi
  if resolved=$(python3 - "$artifacts" "$@" <<'PY'
import json
import os
import sys

executables = {}
with open(sys.argv[1], encoding="utf-8") as messages:
    for line in messages:
        message = json.loads(line)
        target = message.get("target", {})
        if message.get("reason") == "compiler-artifact" and "example" in target.get("kind", []):
            executable = message.get("executable")
            if executable:
                executables[target["name"]] = executable
for name in sys.argv[2:]:
    executable = executables.get(name)
    if not executable or not os.path.isfile(executable):
        sys.exit(f"Cargo did not report a built executable for example {name}")
    if any(char in name + executable for char in "\t\r\n"):
        sys.exit("Cargo example names and executable paths must not contain tabs or newlines")
    print(f"{name}\t{executable}")
PY
  ); then
    :
  else
    status=$?
    rm -f "$artifacts"
    return "$status"
  fi
  rm -f "$artifacts"
  CARGO_EXAMPLE_BINARIES=()
  while IFS=$'\t' read -r example executable; do
    CARGO_EXAMPLE_BINARIES["$example"]=$executable
    printf '[cargo examples] %s: %s\n' "$example" "$executable" >&2
  done <<<"$resolved"
}

cargo_example_binary() {
  [ "$#" = 1 ] || { echo "cargo_example_binary needs one example name" >&2; return 2; }
  local executable=${CARGO_EXAMPLE_BINARIES[$1]:-}
  [ -n "$executable" ] || { echo "example $1 was not built by this harness" >&2; return 1; }
  printf '%s\n' "$executable"
}
