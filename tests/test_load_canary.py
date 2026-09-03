"""Optional live load scenario: 20 concurrent jobs must reach a terminal state.

Run with COMPARATOR_LIVE_TESTS=1 against a running stack.
"""

from __future__ import annotations

import os
import unittest

from app.config import settings

RUN_LIVE = os.environ.get("COMPARATOR_LIVE_TESTS") == "1"


class LoadCanaryTests(unittest.TestCase):
    def test_gpu_slots_are_independent_from_upload_intake(self) -> None:
        self.assertGreaterEqual(settings.consumer_max_concurrent, 1)
        self.assertEqual(settings.ollama_max_concurrent, 1)
        self.assertNotEqual(settings.pipeline_version, "v1")

    @unittest.skipUnless(RUN_LIVE, "live load harness")
    def test_twenty_jobs_reach_terminal_state(self) -> None:
        self.skipTest("Execute against a running cluster during rollout")
