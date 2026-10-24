import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from app import healthcheck


class HealthcheckTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "health.json"
        patcher = patch.object(healthcheck, "HEALTH_FILE", self.path)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_missing_or_malformed_marker_is_not_ready(self):
        self.assertFalse(healthcheck.healthy())
        for content in ("bad json", "null", "{}", '{"ready": true, "updated": "bad"}'):
            self.path.write_text(content)
            self.assertFalse(healthcheck.healthy())

    def test_assignment_is_required(self):
        healthcheck.heartbeat(False)
        self.assertFalse(healthcheck.healthy())
        healthcheck.heartbeat(True)
        self.assertTrue(healthcheck.healthy())

    def test_stalled_polling_loop_is_not_ready(self):
        self.path.write_text(json.dumps({"ready": True, "updated": time.monotonic() - 130}))
        self.assertFalse(healthcheck.healthy())

    def test_marker_from_a_future_clock_is_not_ready(self):
        self.path.write_text(json.dumps({"ready": True, "updated": time.monotonic() + 100}))
        self.assertFalse(healthcheck.healthy())
