#!/usr/bin/env python3
"""Cluster-free contract tests for conditional HDFS HA acceptance."""

import copy
from pathlib import Path
import unittest

from hdfs_ha_gate import HaGateNotConfigured, expected_root, load_config


RUN_ID = "nightly-ha-42"


def complete_environment() -> dict[str, str]:
    root = expected_root(RUN_ID)
    result = {
        "LAB_HDFS_HA_LOCATION": f"hdfs://hdfs@ha-ns{root}",
        "LAB_HDFS_HA_CONFIG_DIR": "/run/terrasync/hadoop-ha",
    }
    for number, host, vmid in ((1, "10.0.0.31", "401"), (2, "10.0.0.32", "402")):
        prefix = f"LAB_HDFS_HA_NAMENODE{number}_"
        result.update(
            {
                prefix + "ID": f"nn{number}",
                prefix + "HOST": host,
                prefix + "VMID": vmid,
                prefix + "NAME": f"hdfs-ha-namenode{number}",
                prefix + "SERVICE": "hadoop-namenode.service",
            }
        )
    return result


class HaGateTests(unittest.TestCase):
    def test_empty_environment_is_an_explicit_non_acceptance_skip(self) -> None:
        with self.assertRaises(HaGateNotConfigured):
            load_config({}, RUN_ID)

    def test_complete_logical_nameservice_configuration_is_accepted(self) -> None:
        config = load_config(complete_environment(), RUN_ID)
        self.assertEqual(config.nameservice, "ha-ns")
        self.assertEqual(config.root, expected_root(RUN_ID))
        self.assertEqual([node.node_id for node in config.nodes], ["nn1", "nn2"])

    def test_every_partial_configuration_is_rejected(self) -> None:
        complete = complete_environment()
        for missing in complete:
            partial = copy.copy(complete)
            del partial[missing]
            with self.subTest(missing=missing), self.assertRaisesRegex(ValueError, "partial"):
                load_config(partial, RUN_ID)

    def test_direct_password_or_broad_root_locations_are_rejected(self) -> None:
        for location in (
            f"hdfs://hdfs@ha-ns:9000{expected_root(RUN_ID)}",
            f"hdfs://hdfs:secret@ha-ns{expected_root(RUN_ID)}",
            "hdfs://hdfs@ha-ns/",
        ):
            environment = complete_environment()
            environment["LAB_HDFS_HA_LOCATION"] = location
            with self.subTest(location=location), self.assertRaises(ValueError):
                load_config(environment, RUN_ID)

    def test_duplicate_nodes_and_unsafe_service_are_rejected(self) -> None:
        environment = complete_environment()
        environment["LAB_HDFS_HA_NAMENODE2_HOST"] = environment[
            "LAB_HDFS_HA_NAMENODE1_HOST"
        ]
        with self.assertRaisesRegex(ValueError, "distinct"):
            load_config(environment, RUN_ID)
        environment = complete_environment()
        environment["LAB_HDFS_HA_NAMENODE1_SERVICE"] = "namenode; poweroff"
        with self.assertRaisesRegex(ValueError, "unsafe"):
            load_config(environment, RUN_ID)

    def test_lab_runner_has_visible_skip_and_restoration_contract(self) -> None:
        runner = (Path(__file__).parent / "lab" / "run-hdfs-ha-e2e.sh").read_text()
        self.assertIn("validation_status == 3", runner)
        self.assertIn('local number="$1" field="$2" variable\n', runner)
        self.assertNotIn(
            'local number="$1" field="$2" variable="LAB_HDFS_HA_', runner
        )
        self.assertIn("trap 'cleanup_ha_run $?' EXIT", runner)
        self.assertIn('service_action stop "$active"', runner)
        self.assertIn('ssh_lab_root "$LAB_HDFS_PVE_HOST"', runner)
        self.assertNotIn('root@"$LAB_HDFS_PVE_HOST"', runner)
        self.assertNotIn("qm stop", runner)
        self.assertNotIn("qm shutdown", runner)
        self.assertIn('[[ "$(ha_state "$remaining_active" || true)" == active ]]', runner)


if __name__ == "__main__":
    unittest.main()
