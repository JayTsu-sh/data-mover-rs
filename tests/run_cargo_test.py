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
    parser.add_argument("--exact", action="store_true")
    parser.add_argument("--ignored", action="store_true")
    args = parser.parse_args(argv)
    target = ["--test", args.test] if args.test else ["--lib"]
    selection = (["--exact"] if args.exact else []) + (["--ignored"] if args.ignored else [])
    command = ["cargo", "test", "--locked", *target, args.filter, "--", *selection]
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
