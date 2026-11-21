import unittest
import json
import logging
import io
import sys
import os

_this_dir = os.path.dirname(os.path.abspath(__file__))
worker_dir = os.path.normpath(
    os.path.join(_this_dir, "..", "..", "api_gateway", "worker")
)
if worker_dir not in sys.path:
    sys.path.insert(0, worker_dir)

from logger import StructuredJsonFormatter, StructuredTextFormatter


class TestStructuredLogging(unittest.TestCase):

    def test_json_formatter_produces_valid_json_with_metadata(self):
        formatter = StructuredJsonFormatter(service_name="scaleflow-test")
        record = logging.LogRecord(
            name="scaleflow.worker",
            level=logging.INFO,
            pathname=__file__,
            lineno=25,
            msg="Task executed successfully",
            args=(),
            exc_info=None
        )
        record.task_id = "105"
        record.event = "TASK_COMPLETED"
        record.duration_ms = 450

        output = formatter.format(record)
        parsed = json.loads(output)

        self.assertEqual(parsed["level"], "INFO")
        self.assertEqual(parsed["service"], "scaleflow-test")
        self.assertEqual(parsed["message"], "Task executed successfully")
        self.assertEqual(parsed["task_id"], "105")
        self.assertEqual(parsed["event"], "TASK_COMPLETED")
        self.assertEqual(parsed["duration_ms"], 450)
        self.assertIn("timestamp", parsed)
        self.assertIn("worker_id", parsed)

    def test_text_formatter_output(self):
        formatter = StructuredTextFormatter()
        record = logging.LogRecord(
            name="scaleflow.worker",
            level=logging.WARN,
            pathname=__file__,
            lineno=50,
            msg="Lease nearing expiration",
            args=(),
            exc_info=None
        )
        record.task_id = "202"
        record.event = "LEASE_WARNING"

        output = formatter.format(record)
        self.assertIn("WARN", output)
        self.assertIn("[LEASE_WARNING]", output)
        self.assertIn("[Task 202]", output)
        self.assertIn("Lease nearing expiration", output)


if __name__ == "__main__":
    unittest.main()
