#!/usr/bin/env python3
"""Check supported facade builds without workspace feature unification masking omissions."""
import argparse
import subprocess

PROTOCOLS = ("local", "nfs", "cifs", "s3", "hdfs")
MATRIX = ("", "acl", "metadata", "traversal", "transfer", "integrity", *(f"backend-{name}" for name in PROTOCOLS), "backend-local,traversal,transfer,integrity", "full")
SDK_PREFIXES = {"s3": ("aws-",), "nfs": ("nfs-rs ",), "cifs": ("smb ",), "hdfs": ("hdfs-native ",)}


def run(*args, capture=False):
    command = ["cargo", *args]
    print("+ " + " ".join(command), flush=True)
    return subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE if capture else None).stdout


def options(features):
    return ["-p", "data-mover", "--no-default-features", *(["--features", features] if features else [])]


def check_graph(features):
    graph = run("tree", "--locked", *options(features), "--edges", "normal,build", "--prefix", "none", "--format", "{p}", capture=True)
    enabled = set(features.split(","))
    for name, prefixes in SDK_PREFIXES.items():
        if "full" in enabled or f"backend-{name}" in enabled:
            continue
        offenders = [line for line in graph.splitlines() if line.startswith(prefixes)]
        if offenders:
            raise RuntimeError(f"{features or 'minimal'} unexpectedly compiles {name}: {offenders}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--graphs-only", action="store_true")
    args = parser.parse_args()
    for features in MATRIX:
        check_graph(features)
        if not args.graphs_only:
            run("check", "--locked", *options(features), "--lib")
    if not args.graphs_only:
        for features in ("", "backend-local,traversal,transfer,integrity"):
            run("check", "--locked", *options(features), "--all-targets")
            run("test", "--locked", *options(features), "--test", "backend_features", "--test", "observed_entry_snapshot", "--test", "storage_model_contract")
        run("test", "--locked", "-p", "data-mover-storage-core", "--test", "adapter_contract")
    print("Feature matrix passed", flush=True)


if __name__ == "__main__":
    main()
