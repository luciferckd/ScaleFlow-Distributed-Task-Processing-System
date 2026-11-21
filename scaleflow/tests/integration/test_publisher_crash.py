import unittest
from unittest.mock import MagicMock, patch, call
import sys
import os

_this_dir = os.path.dirname(os.path.abspath(__file__))
worker_dir = os.path.normpath(
    os.path.join(_this_dir, "..", "..", "api_gateway", "worker")
)
if worker_dir not in sys.path:
    sys.path.insert(0, worker_dir)

import main


class TestPublisherCrashIntegration(unittest.TestCase):
    """
    Integration simulation of publisher crashes and recovery.
    Verifies that regardless of whether the publisher crashes before or after Redis publishing,
    no duplicate events are pushed to the Redis stream and the outbox reaches a consistent published state.
    """

    @patch("main.redis_client")
    def test_full_crash_and_recovery_cycle(self, mock_redis):
        # 1. Setup shared mock Redis state
        redis_store = {}
        stream_events = []

        def mock_eval(script, numkeys, marker_key, stream_name, *args):
            if marker_key in redis_store:
                return redis_store[marker_key]
            # Generate new message ID
            msg_id = f"1700000000000-{len(stream_events)}"
            stream_events.append((stream_name, args))
            redis_store[marker_key] = msg_id
            return msg_id

        mock_redis.eval.side_effect = mock_eval

        # 2. Simulate publisher crash after Redis publish (Crash Point 2)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        outbox_row = {
            "id": 501,
            "task_id": 99,
            "event_type": "RETRY",
            "payload": {"taskId": "99", "retry_count": "1"}
        }
        mock_cursor.fetchone.return_value = outbox_row

        with patch("os._exit") as mock_exit:
            main.publish_single_outbox_event(mock_conn, mock_cursor, crash_simulation="after_redis")
            mock_exit.assert_called_once_with(1)

        # After crash: 1 event reached stream, marker is set in Redis, but MySQL transaction rolled back
        self.assertEqual(len(stream_events), 1)
        self.assertIn("scaleflow:outbox:published:501", redis_store)
        mock_conn.rollback.assert_called()

        # 3. Simulate publisher restarted (Recovery phase)
        mock_recovery_conn = MagicMock()
        mock_recovery_cursor = MagicMock()
        mock_recovery_cursor.fetchone.return_value = outbox_row

        recovered_id = main.publish_single_outbox_event(
            mock_recovery_conn, mock_recovery_cursor, crash_simulation=None
        )

        # Verified: recovered_id is 501
        self.assertEqual(recovered_id, 501)
        # CRITICAL ASSERTION: stream_events length is STILL 1 (ZERO duplicate messages added!)
        self.assertEqual(len(stream_events), 1)
        # MySQL record was updated and committed
        mock_recovery_cursor.execute.assert_has_calls([
            call("""
            SELECT id, task_id, event_type, payload
            FROM task_outbox
            WHERE published = FALSE
            ORDER BY id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """),
            call("""
            UPDATE task_outbox
            SET published = TRUE,
                published_at = NOW()
            WHERE id = %s
            """, (501,))
        ])
        mock_recovery_conn.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
