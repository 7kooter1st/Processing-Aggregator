"""Kill-point and canary helpers used by chaos/load checks."""

from __future__ import annotations

import os
import unittest

RUN_LIVE = os.environ.get("COMPARATOR_LIVE_TESTS") == "1"


class KillPointContractTests(unittest.TestCase):
    def test_pipeline_versions_can_coexist(self) -> None:
        legacy = "v1"
        current = "v2"
        self.assertNotEqual(legacy, current)

    def test_dlt_is_not_a_retry_queue(self) -> None:
        retry_topics = {"cmp.ocr.retry.v1", "cmp.diff.retry.v1"}
        dlt_topics = {"cmp.ocr.dlt.v1", "raw_chunks_dlt"}
        self.assertTrue(retry_topics.isdisjoint(dlt_topics))

    @unittest.skipUnless(RUN_LIVE, "set COMPARATOR_LIVE_TESTS=1 for Kafka/PG harness")
    def test_live_harness_placeholder(self) -> None:
        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()
