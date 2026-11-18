import unittest
from unittest.mock import MagicMock, patch, call
import sys
import os
import json

# Add worker directory to sys.path so we can import worker modules
worker_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "api_gateway", "worker")
)
if worker_dir not in sys.path:
    sys.path.insert(0, worker_dir)

import main


class TestPhase1ReliabilityHardening(unittest.TestCase):

    def setUp(self):
        # Reset any environment variables
        if "PUBLISHER_CRASH_TEST" in os.environ:
            del os.environ["PUBLISHER_CRASH_TEST"]

    # ──────────────────────────────────────────────────────────
    # Task 1: Idempotent Outbox publishing using outbox_id
    # ──────────────────────────────────────────────────────────
    @patch("main.redis_client")
    def test_outbox_publish_script_lua_structure(self, mock_redis):
        """Verify the Lua script checks KEYS[1] before executing XADD."""
        lua_script = main.OUTBOX_PUBLISH_SCRIPT
        self.assertIn("redis.call('GET', KEYS[1])", lua_script)
        self.assertIn("'XADD'", lua_script)
        self.assertIn("KEYS[2]", lua_script)
        self.assertIn("unpack(ARGV)", lua_script)
        self.assertIn("redis.call('SET', KEYS[1], message_id", lua_script)

    @patch("main.redis_client")
    @patch("main.get_db_connection")
    def test_idempotent_outbox_publish_new_event(self, mock_get_db, mock_redis):
        """Test publishing a new event to Redis and committing MySQL record."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        event_data = {
            "id": 101,
            "task_id": 55,
            "event_type": "RETRY",
            "payload": json.dumps({"taskId": "55", "retry_count": "1"})
        }
        mock_cursor.fetchone.return_value = event_data
        mock_redis.eval.return_value = b"1700000000000-0"
        mock_redis.get.return_value = b"1700000000000-0"

        event_id = main.publish_single_outbox_event(mock_conn, mock_cursor)

        self.assertEqual(event_id, 101)
        # Verify redis.eval called with idempotent script and marker key
        mock_redis.eval.assert_called_once()
        args = mock_redis.eval.call_args[0]
        self.assertEqual(args[0], main.OUTBOX_PUBLISH_SCRIPT)
        self.assertEqual(args[1], 2)  # numkeys
        self.assertEqual(args[2], "scaleflow:outbox:published:101")
        self.assertEqual(args[3], main.STREAM_NAME)

        # Verify DB was marked published
        mock_cursor.execute.assert_has_calls([
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
            """, (101,))
        ])
        mock_conn.commit.assert_called()

    @patch("main.redis_client")
    @patch("main.get_db_connection")
    def test_idempotent_outbox_publish_duplicate_skips_xadd(self, mock_get_db, mock_redis):
        """Test that re-publishing returns the existing message_id without new stream messages."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        event_data = {
            "id": 102,
            "task_id": 56,
            "event_type": "DEAD_LETTER",
            "payload": {"taskId": "56", "error": "Max retries"}
        }
        mock_cursor.fetchone.return_value = event_data
        # Simulated Lua script returning the existing Redis message ID
        mock_redis.eval.return_value = "1700000000000-0"

        event_id = main.publish_single_outbox_event(mock_conn, mock_cursor)

        self.assertEqual(event_id, 102)
        mock_redis.eval.assert_called_once()
        # Verify target stream was dead letter stream
        self.assertEqual(mock_redis.eval.call_args[0][3], main.DEAD_LETTER_STREAM)
        mock_conn.commit.assert_called()

    # ──────────────────────────────────────────────────────────
    # Task 2: Duplicate Redis message detection
    # ──────────────────────────────────────────────────────────
    @patch("main.redis_client")
    def test_duplicate_detection_by_message_id(self, mock_redis):
        """Worker must detect message_id already in Redis processed set."""
        mock_redis.get.side_effect = lambda k: b"1" if k == "scaleflow:processed:msg:1700-1" else None

        is_dup, reason = main.is_duplicate_message("1700-1", {})
        self.assertTrue(is_dup)
        self.assertIn("1700-1 already processed", reason)

    @patch("main.redis_client")
    def test_duplicate_detection_by_outbox_id(self, mock_redis):
        """Worker must detect outbox_id already processed."""
        mock_redis.get.side_effect = lambda k: b"1" if k == "scaleflow:processed:outbox:999" else None

        is_dup, reason = main.is_duplicate_message("1700-2", {"outbox_id": "999"})
        self.assertTrue(is_dup)
        self.assertIn("Outbox event 999 already processed", reason)

    @patch("main.redis_client")
    @patch("main.get_task_details")
    def test_process_task_skips_and_acks_duplicate(self, mock_get_task, mock_redis):
        """When duplicate message arrives, process_task ACKs and returns immediately."""
        mock_redis.get.return_value = b"1"  # message marked as processed

        main.process_task("10", "1700-dup", {"outbox_id": "88"})

        # Should NOT fetch task details or execute task
        mock_get_task.assert_not_called()
        # Should immediately acknowledge in Redis
        mock_redis.xack.assert_called_once_with(main.STREAM_NAME, main.GROUP_NAME, "1700-dup")

    @patch("main.redis_client")
    @patch("main.get_task_details")
    def test_process_task_skips_already_completed_task(self, mock_get_task, mock_redis):
        """When DB status is already COMPLETED, message is acknowledged as duplicate."""
        mock_redis.get.return_value = None  # not in processed cache
        mock_get_task.return_value = {
            "id": 15,
            "status": "COMPLETED",
            "retry_count": 0,
            "max_retries": 3
        }

        main.process_task("15", "1700-comp", {})

        # Should acknowledge duplicate completed task
        mock_redis.xack.assert_called_once_with(main.STREAM_NAME, main.GROUP_NAME, "1700-comp")
        # Should record in Redis processed cache to prevent future DB lookups
        mock_redis.set.assert_called_with("scaleflow:processed:msg:1700-comp", "1", ex=86400)

    @patch("main.redis_client")
    @patch("main.get_task_details")
    def test_process_task_skips_stale_retry_message(self, mock_get_task, mock_redis):
        """When incoming retry count is smaller than current DB retry count, message is stale."""
        mock_redis.get.return_value = None
        mock_get_task.return_value = {
            "id": 20,
            "status": "PENDING",
            "retry_count": 2,
            "max_retries": 3
        }

        # Message is for retry attempt 1, but task is already at retry 2
        main.process_task("20", "1700-stale", {"retry_count": "1"})

        mock_redis.xack.assert_called_once_with(main.STREAM_NAME, main.GROUP_NAME, "1700-stale")

    # ──────────────────────────────────────────────────────────
    # Task 3: Publisher crash recovery testing
    # ──────────────────────────────────────────────────────────
    @patch("os._exit")
    @patch("main.redis_client")
    def test_publisher_crash_before_redis(self, mock_redis, mock_exit):
        """Simulate publisher crashing before Redis XADD."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {
            "id": 201,
            "task_id": 80,
            "event_type": "RETRY",
            "payload": {"taskId": "80"}
        }

        main.publish_single_outbox_event(
            mock_conn, mock_cursor, crash_simulation="before_redis"
        )

        mock_exit.assert_called_once_with(1)
        mock_redis.eval.assert_not_called()
        mock_conn.rollback.assert_called()

    @patch("os._exit")
    @patch("main.redis_client")
    def test_publisher_crash_after_redis_before_mysql(self, mock_redis, mock_exit):
        """Simulate publisher crashing after Redis XADD, before MySQL UPDATE."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {
            "id": 202,
            "task_id": 81,
            "event_type": "RETRY",
            "payload": {"taskId": "81"}
        }
        mock_redis.eval.return_value = "1700-new-msg"

        main.publish_single_outbox_event(
            mock_conn, mock_cursor, crash_simulation="after_redis"
        )

        mock_exit.assert_called_once_with(1)
        mock_redis.eval.assert_called_once()
        mock_conn.rollback.assert_called()

    @patch("main.redis_client")
    def test_publisher_recovery_after_crash(self, mock_redis):
        """
        Full recovery flow: after a crash between Redis publish and MySQL commit,
        the restarted publisher must pick up the event, invoke OUTBOX_PUBLISH_SCRIPT
        which returns the existing message ID (no duplicate), and mark MySQL published=TRUE.
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        event_data = {
            "id": 203,
            "task_id": 82,
            "event_type": "RETRY",
            "payload": {"taskId": "82"}
        }
        mock_cursor.fetchone.return_value = event_data
        # Simulated Lua script recognizing existing marker key in Redis:
        mock_redis.eval.return_value = "1700-existing-msg"

        event_id = main.publish_single_outbox_event(mock_conn, mock_cursor)

        self.assertEqual(event_id, 203)
        mock_cursor.execute.assert_has_calls([
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
            """, (203,))
        ])
        mock_conn.commit.assert_called_once()

    # ──────────────────────────────────────────────────────────
    # Task 4: Multi-publisher coordination
    # ──────────────────────────────────────────────────────────
    @patch("main.redis_client")
    def test_multi_publisher_skip_locked_query(self, mock_redis):
        """Ensure SELECT query uses SKIP LOCKED so concurrent publishers don't block."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # No pending events

        event_id = main.publish_single_outbox_event(mock_conn, mock_cursor)

        self.assertIsNone(event_id)
        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        self.assertIn("FOR UPDATE SKIP LOCKED", query)

    @patch("main.redis_client")
    def test_concurrent_publishers_independent_events(self, mock_redis):
        """Simulate two concurrent publishers processing distinct events without collision."""
        mock_conn1 = MagicMock()
        mock_cursor1 = MagicMock()
        mock_conn2 = MagicMock()
        mock_cursor2 = MagicMock()

        event1 = {"id": 301, "task_id": 101, "event_type": "RETRY", "payload": {"taskId": "101"}}
        event2 = {"id": 302, "task_id": 102, "event_type": "RETRY", "payload": {"taskId": "102"}}

        mock_cursor1.fetchone.return_value = event1
        mock_cursor2.fetchone.return_value = event2
        mock_redis.eval.side_effect = ["1700-msg-1", "1700-msg-2"]

        res1 = main.publish_single_outbox_event(mock_conn1, mock_cursor1)
        res2 = main.publish_single_outbox_event(mock_conn2, mock_cursor2)

        self.assertEqual(res1, 301)
        self.assertEqual(res2, 302)
        mock_conn1.commit.assert_called_once()
        mock_conn2.commit.assert_called_once()

    # ──────────────────────────────────────────────────────────
    # Task 5: Periodic pending-message recovery
    # ──────────────────────────────────────────────────────────
    @patch("main.process_task")
    @patch("main.recover_orphaned_db_tasks")
    @patch("main.redis_client")
    def test_xautoclaim_pagination_recovers_all_messages(
        self, mock_redis, mock_recover_db, mock_process
    ):
        """Test that recover_pending_tasks pages through PEL until next_id is 0-0."""
        # First page returns 2 messages and next_id = "1700-10"
        page1 = (
            "1700-10",
            [
                (b"1700-1", {b"taskId": b"101"}),
                (b"1700-2", {b"taskId": b"102"}),
            ],
            []
        )
        # Second page returns 1 message and next_id = "0-0" (done)
        page2 = (
            "0-0",
            [
                (b"1700-3", {b"taskId": b"103"}),
            ],
            []
        )
        mock_redis.xautoclaim.side_effect = [page1, page2]

        main.recover_pending_tasks(min_idle_time_ms=60000, batch_size=2)

        self.assertEqual(mock_redis.xautoclaim.call_count, 2)
        self.assertEqual(mock_process.call_count, 3)
        mock_recover_db.assert_called_once()

    @patch("main.invalidate_cache")
    @patch("main.reset_expired_task")
    @patch("main.get_expired_processing_tasks")
    def test_recover_orphaned_db_tasks(self, mock_get_expired, mock_reset, mock_cache):
        """Test recovering orphaned MySQL tasks stuck in PROCESSING with expired leases."""
        mock_get_expired.return_value = [
            {"id": 401, "locked_by": "dead-worker-1"},
            {"id": 402, "locked_by": "dead-worker-2"}
        ]
        mock_reset.return_value = True

        main.recover_orphaned_db_tasks()

        self.assertEqual(mock_reset.call_count, 2)
        mock_reset.assert_has_calls([call(401), call(402)])
        self.assertEqual(mock_cache.call_count, 2)

    @patch("main.recover_pending_tasks")
    def test_periodic_recovery_worker_loop_and_stop(self, mock_recover):
        """Test periodic recovery worker executes and shuts down gracefully via stop_event."""
        import threading
        stop_event = threading.Event()
        stop_event.set()  # Stop immediately after one iteration

        main.periodic_pending_recovery_worker(interval_seconds=1, stop_event=stop_event)

        mock_recover.assert_called_once()


if __name__ == "__main__":
    unittest.main()
