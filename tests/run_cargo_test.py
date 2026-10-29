#!/usr/bin/env python3
"""Run a selected Cargo test gate, refusing an empty or entirely ignored selection."""

import argparse
import re
import subprocess
import sys


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("filter")
    parser.add_argument("--test", help="integration target (defaults to library tests)")
    parser.add_argument("--package", "-p", help="workspace package (inferred for legacy module filters)")
    parser.add_argument("--features", help="features enabled for this test command")
    parser.add_argument("--exact", action="store_true")
    parser.add_argument("--ignored", action="store_true")
    args = parser.parse_args(argv)
    target = ["--test", args.test] if args.test else ["--lib"]
    selection = (["--exact"] if args.exact else []) + (["--ignored"] if args.ignored else [])
    package = args.package
    selected_filter = args.filter
    if not package and not args.test:
        for backend in ("local", "nfs", "cifs", "s3", "hdfs"):
            prefix = f"storage::backends::{backend}::"
            if selected_filter.startswith(prefix):
                package = f"data-mover-backend-{backend}"
                selected_filter = "backend::" + selected_filter.removeprefix(prefix)
                break
        if not package:
            if selected_filter.startswith("model::acl::"):
                package, selected_filter = "data-mover-acl", selected_filter.removeprefix("model::")
            elif selected_filter.startswith("model::"):
                package, selected_filter = "data-mover-model", selected_filter.removeprefix("model::")
            elif selected_filter.split("::")[0] in {"storage", "runtime", "qos", "transfer_concurrency"}:
                package = "data-mover-storage-core"
            elif selected_filter.split("::")[0] in {"filter", "filter_traversal"}:
                package = "data-mover-traversal"
            elif selected_filter.split("::")[0] in {"transfer", "traversal", "metadata", "integrity"}:
                package = "data-mover-" + selected_filter.split("::")[0]
    cargo_options = (["-p", package] if package else []) + (["--features", args.features] if args.features else [])
    command = ["cargo", "test", "--locked", *target, *cargo_options, selected_filter, "--", *selection]
    listed = subprocess.run([*command, "--list"], text=True, stdout=subprocess.PIPE)
    if listed.returncode:
        return listed.returncode
    if not any(line.endswith(": test") for line in listed.stdout.splitlines()):
        print(f"Cargo test gate selected no tests: {args.filter}", file=sys.stderr)
        return 2
    completed = subprocess.run(
        [*command, "--test-threads=1"], text=True, stdout=subprocess.PIPE
    )
    print(completed.stdout, end="", flush=True)
    if completed.returncode:
        return completed.returncode
    if not re.search(r"test result: ok\. [1-9][0-9]* passed;", completed.stdout):
        print(f"Cargo test gate executed no tests: {args.filter}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
