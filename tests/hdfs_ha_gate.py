#!/usr/bin/env python3
"""Validate the opt-in real HDFS HA lab gate without exposing config values."""

from __future__ import annotations

import argparse
import ipaddress
import os
import re
import sys
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


NODE_FIELDS = ("ID", "HOST", "VMID", "NAME", "SERVICE")
REQUIRED = (
    "LAB_HDFS_HA_LOCATION",
    "LAB_HDFS_HA_CONFIG_DIR",
    *(f"LAB_HDFS_HA_NAMENODE{number}_{field}" for number in (1, 2) for field in NODE_FIELDS),
)
SAFE_TOKEN = re.compile(r"^[A-Za-z0-9._-]{1,80}$")


class HaGateNotConfigured(Exception):
    """No HA variables were supplied, so acceptance must remain unclaimed."""


@dataclass(frozen=True)
class HaNode:
    node_id: str
    host: str
    vmid: int
    name: str
    service: str


@dataclass(frozen=True)
class HaGateConfig:
    location: str
    config_dir: Path
    nameservice: str
    root: str
    nodes: tuple[HaNode, HaNode]


def expected_root(run_id: str) -> str:
    if not re.fullmatch(r"(?:nightly|release)-[A-Za-z0-9._-]{1,80}", run_id):
        raise ValueError("invalid HA lab run id")
    if ".." in run_id:
        raise ValueError("invalid HA lab run id")
    kind = run_id.split("-", 1)[0]
    return f"/tmp/data-mover-{kind}/{run_id}/hdfs/ha"


def load_config(environment: Mapping[str, str], run_id: str) -> HaGateConfig:
    values = {name: environment.get(name, "").strip() for name in REQUIRED}
    configured = [name for name, value in values.items() if value]
    if not configured:
        raise HaGateNotConfigured
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise ValueError("partial HA configuration; missing: " + ", ".join(missing))

    parsed = urllib.parse.urlsplit(values["LAB_HDFS_HA_LOCATION"])
    if (
        parsed.scheme != "hdfs"
        or not parsed.username
        or parsed.password is not None
        or parsed.hostname is None
        or parsed.port is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("HA location must be a password-free logical hdfs://user@nameservice URL")
    root = expected_root(run_id)
    if parsed.path.rstrip("/") != root:
        raise ValueError("HA location is not confined to the expected run root")

    config_dir = Path(values["LAB_HDFS_HA_CONFIG_DIR"])
    if not config_dir.is_absolute():
        raise ValueError("HA config directory must be absolute")

    nodes = []
    for number in (1, 2):
        prefix = f"LAB_HDFS_HA_NAMENODE{number}_"
        node_id = values[prefix + "ID"]
        name = values[prefix + "NAME"]
        service = values[prefix + "SERVICE"]
        if not all(SAFE_TOKEN.fullmatch(value) for value in (node_id, name, service)):
            raise ValueError(f"NameNode {number} identity contains unsafe characters")
        try:
            host = str(ipaddress.ip_address(values[prefix + "HOST"]))
            vmid = int(values[prefix + "VMID"])
        except ValueError as error:
            raise ValueError(f"NameNode {number} host or VMID is invalid") from error
        if not 1 <= vmid <= 999_999_999:
            raise ValueError(f"NameNode {number} VMID is outside the accepted range")
        nodes.append(HaNode(node_id, host, vmid, name, service))

    if nodes[0].node_id == nodes[1].node_id or nodes[0].host == nodes[1].host:
        raise ValueError("HA NameNodes must have distinct IDs and hosts")
    return HaGateConfig(
        location=values["LAB_HDFS_HA_LOCATION"],
        config_dir=config_dir,
        nameservice=parsed.hostname,
        root=root,
        nodes=(nodes[0], nodes[1]),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    try:
        config = load_config(os.environ, args.run_id)
    except HaGateNotConfigured:
        print("HDFS HA acceptance: NOT RUN (no HA topology configured)")
        return 3
    except ValueError as error:
        print(f"HDFS HA gate configuration rejected: {error}", file=sys.stderr)
        return 2
    print(f"{config.nameservice}\t{config.root}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
