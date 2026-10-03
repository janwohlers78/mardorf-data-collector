"""Lossless diagnostic compression never changes native weather or clocks."""

import copy
import unittest
from mardorf_collector.wp13.core_v1 import CollectorError
from mardorf_collector.wp13.status_codec_v1 import (
    compact_outcomes,
    restore_outcomes,
    EXTENSION,
)


class StatusCodecTests(unittest.TestCase):
    def example(self):
        return {
            "envelope": {"extensions": {}, "raw_objects": []},
            "raw_bytes": {"original": b"immutable provider response"},
            "fields": [
                {
                    "value_native": 9.5,
                    "unit_native": "degF",
                    "time": {"first_seen_at_utc": "2026-10-01T19:09:22Z"},
                }
            ],
            "fragment_bytes": {"native": b"original exact native sidecar"},
            "source_status": [
                {
                    "source_index": 0,
                    "status": "received",
                    "observed_at_utc": "2026-10-01T19:09:22Z",
                    "fields": [
                        {
                            "native_parameter": "unmapped_quantity",
                            "row_pointer": f"/sensors/0/data/{i}/unmapped_quantity",
                            "status": "raw_only",
                        }
                        for i in range(800)
                    ],
                }
            ],
        }

    def test_complete_outcomes_roundtrip_and_native_identity_unchanged(self):
        original = self.example()
        before = copy.deepcopy(original)
        packed = compact_outcomes(original)
        self.assertEqual(original, before)
        self.assertEqual(packed["fields"], original["fields"])
        self.assertEqual(packed["fragment_bytes"], original["fragment_bytes"])
        self.assertEqual(
            packed["raw_bytes"]["original"], original["raw_bytes"]["original"]
        )
        self.assertLess(sum(map(len, packed["raw_bytes"].values())), 6000)
        self.assertEqual(
            restore_outcomes(
                packed["envelope"], packed["source_status"], packed["raw_bytes"]
            ),
            original["source_status"],
        )
        with self.assertRaises(CollectorError):
            compact_outcomes(packed)

    def test_tampering_and_decode_limits_reject_before_unbounded_allocation(self):
        for mutation in ("bytes", "decoded_size", "counts", "hash", "mixed"):
            out = compact_outcomes(self.example())
            ref = out["envelope"]["extensions"][EXTENSION]
            if mutation == "bytes":
                out["raw_bytes"][ref["raw_object_id"]] = b"bad"
            elif mutation == "decoded_size":
                ref["decoded_bytes"] = 17 * 1024**2
            elif mutation == "counts":
                ref["source_field_counts"] = []
            elif mutation == "hash":
                ref["decoded_sha256"] = "0" * 64
            else:
                out["source_status"][0]["fields"] = [{"contradiction": True}]
            with self.subTest(mutation=mutation), self.assertRaises(CollectorError):
                restore_outcomes(
                    out["envelope"], out["source_status"], out["raw_bytes"]
                )

    def test_small_and_failed_transfers_keep_original_layout(self):
        original = self.example()
        original["source_status"][0]["fields"] = []
        self.assertEqual(compact_outcomes(original), original)
        original["envelope"] = None
        self.assertEqual(compact_outcomes(original), original)


if __name__ == "__main__":
    unittest.main()
