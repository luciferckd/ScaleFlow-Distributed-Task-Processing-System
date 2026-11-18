import redis
import os
import socket
import time
import threading
import json

from redis_client import redis_client, connect_redis
from database import (
    get_db_connection,
    get_task_details,
    update_task_status,
    update_retry_status,
    update_retry_status_with_outbox,
    claim_task,
    release_task_lock,
    renew_task_lease,
    complete_task,
    get_expired_processing_tasks,
    reset_expired_task
)

STREAM_NAME = "scaleflow:tasks"
GROUP_NAME = "scaleflow-workers"
CONSUMER_NAME = os.getenv("WORKER_ID") or socket.gethostname()
PUBLISHER_ID = os.getenv("PUBLISHER_ID") or CONSUMER_NAME
DEAD_LETTER_STREAM = "scaleflow:tasks:dead"

# Task 1: Idempotent Outbox Publishing Script
# Checks if marker_key exists; if so, returns existing message_id without publishing duplicate XADD.
# Otherwise, issues XADD and stores the marker key with 7 days TTL (604800s).
OUTBOX_PUBLISH_SCRIPT = """
local existing_id = redis.call('GET', KEYS[1])

if existing_id then
    return existing_id
end

local message_id = redis.call(
    'XADD',
    KEYS[2],
    '*',
    unpack(ARGV)
)

redis.call('SET', KEYS[1], message_id, 'EX', 604800)

return message_id
"""


def create_consumer_group():
    try:
        redis_client.xgroup_create(
            STREAM_NAME,
            GROUP_NAME,
            id="0",
            mkstream=True
        )
        print("Consumer Group Created Successfully")

    except redis.exceptions.ResponseError as error:
        if "BUSYGROUP" in str(error):
            print("Consumer Group Already Exists")
        else:
            raise


def invalidate_cache(task_id):
    try:
        redis_client.delete(f"task:{task_id}")
        redis_client.delete("tasks:all")
    except Exception as error:
        print(f"Warning: Failed to invalidate cache for task {task_id}: {error}")


# ── Task 2: Duplicate Redis Message Detection Helpers ──

def is_duplicate_message(message_id, data=None):
    """
    Check if this Redis message has already been processed or is a duplicate.
    Checks:
    1. Redis processed message marker (scaleflow:processed:msg:{message_id})
    2. Outbox event deduplication marker (scaleflow:processed:outbox:{outbox_id})
    Returns (is_duplicate: bool, reason: str)
    """
    if data is None:
        data = {}

    try:
        # Check 1: Processed message ID
        if redis_client.get(f"scaleflow:processed:msg:{message_id}"):
            return True, f"Redis message_id {message_id} already processed"

        # Check 2: Processed outbox ID
        outbox_id = data.get("outbox_id")
        if outbox_id and redis_client.get(f"scaleflow:processed:outbox:{outbox_id}"):
            return True, f"Outbox event {outbox_id} already processed"
    except Exception as error:
        print(f"Warning: Redis check in duplicate detection failed: {error}")

    return False, ""


def mark_message_processed(message_id, outbox_id=None):
    """
    Mark message and optional outbox_id as processed in Redis with 24-hour TTL.
    Prevents duplicate reprocessing if Redis redelivers the message.
    """
    try:
        redis_client.set(f"scaleflow:processed:msg:{message_id}", "1", ex=86400)
        if outbox_id:
            redis_client.set(f"scaleflow:processed:outbox:{outbox_id}", "1", ex=86400)
    except Exception as error:
        print(f"Warning: Failed to mark message processed in Redis: {error}")


class LeaseHeartbeat:
    """
    Background thread that renews a task lease.
    Signals the main thread if lease ownership is lost.
    """

    def __init__(self, task_id, worker_id, interval=20, extend_seconds=60):
        self.task_id = task_id
        self.worker_id = worker_id
        self.interval = interval
        self.extend_seconds = extend_seconds

        self.stop_event = threading.Event()
        self.lease_lost_event = threading.Event()

        self.thread = threading.Thread(
            target=self._run,
            daemon=True
        )

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join()

    def lease_is_lost(self):
        return self.lease_lost_event.is_set()

    def _run(self):
        while not self.stop_event.wait(self.interval):
            try:
                renewed = renew_task_lease(
                    self.task_id,
                    self.worker_id,
                    extend_seconds=self.extend_seconds
                )

                if renewed:
                    print(
                        f"[Heartbeat] Task {self.task_id}: "
                        f"lease renewed by {self.worker_id}"
                    )
                else:
                    print(
                        f"[Heartbeat] Task {self.task_id}: "
                        "LEASE LOST"
                    )
                    self.lease_lost_event.set()
                    break

            except Exception as error:
                print(
                    f"[Heartbeat] Task {self.task_id}: "
                    f"renewal error: {error}"
                )
                self.lease_lost_event.set()
                break


def execute_task(task_id):
    print(f"Processing task {task_id}...")
    print(f"Task {task_id}: Simulating work (12 seconds)...")
    time.sleep(12)
    print(f"Task {task_id} execution successful")


def process_task(task_id, message_id, data=None):
    """
    Single unified task processor.

    Flow:
    0. Duplicate Redis message detection (Redis & Outbox dedup)
    1. Check if task exists and is not already COMPLETED or stale retry
    2. Atomically claim the task (MySQL lock)
    3. Crash simulation check (CRASH_TEST=true)
    4. Execute the task with heartbeat
    5. Mark COMPLETED, mark processed in Redis, release lock
    6. ACK the Redis message

    On failure:
    - Retry if retry_count < max_retries via transactional outbox
    - Move to Dead Letter Stream via transactional outbox if max retries exceeded
    - ACK original message only after retry/DLQ outbox record is committed
    """
    if data is None:
        data = {}

    worker_id = CONSUMER_NAME
    outbox_id = data.get("outbox_id")

    # ── Step 0: Duplicate Redis message detection ──
    is_dup, reason = is_duplicate_message(message_id, data)
    if is_dup:
        print(f"[Duplicate Detected] {reason}. Skipping execution and acknowledging.")
        redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
        return

    # ── Step 1: Fetch task and validate ──
    task = get_task_details(task_id)

    if not task:
        print(f"Task {task_id} not found in database. Acknowledging stale message.")
        mark_message_processed(message_id, outbox_id)
        redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
        return

    if task["status"] == "COMPLETED":
        print(f"[Duplicate Detected] Task {task_id} already COMPLETED in database. Skipping duplicate.")
        mark_message_processed(message_id, outbox_id)
        redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
        return

    # Check for stale retry messages
    msg_retry_count = data.get("retry_count")
    if msg_retry_count is not None:
        try:
            if int(task["retry_count"] or 0) > int(msg_retry_count):
                print(
                    f"[Duplicate Detected] Stale retry message for task {task_id} "
                    f"(message retry={msg_retry_count}, current db retry={task['retry_count']}). Acknowledging."
                )
                mark_message_processed(message_id, outbox_id)
                redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
                return
        except ValueError:
            pass

    # ── Step 2: Atomic claim via MySQL ──
    retry_count = task["retry_count"] or 0
    max_retries = task["max_retries"] or 3

    claimed = claim_task(
        task_id,
        worker_id,
        lease_seconds=60
    )

    if not claimed:
        print(f"Task {task_id} is already claimed by another worker. Skipping.")
        return  # Do NOT ack — let the active lock holder process it

    print(f"Task {task_id} claimed by {worker_id}")

    # ── Step 3: Crash simulation ──
    if os.getenv("CRASH_TEST") == "true":
        print("CRASH_TEST=true — Simulating worker crash before execution...")
        print(f"Task {task_id} is PROCESSING with lock held by {worker_id}")
        print("Worker crashing NOW. Task will remain PROCESSING with expired lease.")
        os._exit(1)

    # ── Step 4: Execute the task with lease heartbeat ──
    heartbeat = LeaseHeartbeat(
        task_id,
        worker_id,
        interval=20,
        extend_seconds=60
    )
    heartbeat.start()

    try:
        execute_task(task_id)

        # Check if heartbeat detected lease loss during execution
        if heartbeat.lease_is_lost():
            print(f"Task {task_id} — lease was lost during execution. Aborting completion.")
            return

        # ── Step 5: Atomically mark COMPLETED and release lock ──
        completed = complete_task(
            task_id,
            worker_id,
            retry_count
        )

        if not completed:
            print(f"Task {task_id} — lock lost during execution. Another worker may have reclaimed it.")
            return

        # Invalidate cache
        invalidate_cache(task_id)

        # Mark message as processed for duplicate detection
        mark_message_processed(message_id, outbox_id)

        print(f"Task {task_id} COMPLETED")

        # ── Step 6: ACK the message ──
        redis_client.xack(
            STREAM_NAME,
            GROUP_NAME,
            message_id
        )

        print("Task Acknowledged")

    except Exception as error:
        print(f"Task {task_id} Processing Failed: {error}")

        task = get_task_details(task_id)

        if not task:
            print(f"Task {task_id} not found during error handling.")
            return

        retry_count = (task["retry_count"] or 0) + 1
        max_retries = task["max_retries"] or 3

        print(f"Retry Attempt: {retry_count}/{max_retries}")

        if retry_count <= max_retries:
            status = "PENDING"
        else:
            status = "FAILED"

        updated = update_retry_status_with_outbox(
            task_id,
            worker_id,
            retry_count,
            status,
            error
        )

        if not updated:
            print("Task lock lost. Leaving message pending.")
            return

        invalidate_cache(task_id)

        if status == "PENDING":
            print("Retry event saved to outbox")
        else:
            print("Maximum retries reached")
            print("Dead letter event saved to outbox")

        mark_message_processed(message_id, outbox_id)

        redis_client.xack(
            STREAM_NAME,
            GROUP_NAME,
            message_id
        )

        print("Original message acknowledged")

    finally:
        heartbeat.stop()


# ── Task 1, 3, 4: Outbox Publishing with Idempotency, Crash Hooks & Multi-Publisher Coordination ──

def publish_single_outbox_event(connection=None, cursor=None, crash_simulation=None):
    """
    Publish a single pending outbox event to Redis.
    Uses 'FOR UPDATE SKIP LOCKED' for non-blocking multi-publisher coordination.
    Uses OUTBOX_PUBLISH_SCRIPT for idempotent publishing using outbox_id.
    Includes crash simulation hooks for publisher crash recovery testing.
    """
    close_connection = False
    if connection is None:
        connection = get_db_connection()
        close_connection = True

    close_cursor = False
    if cursor is None:
        cursor = connection.cursor(dictionary=True)
        close_cursor = True

    try:
        # Multi-publisher coordination: SELECT ... FOR UPDATE SKIP LOCKED
        # Skips rows currently locked by other publishers, enabling high-throughput parallel publishing
        cursor.execute(
            """
            SELECT id, task_id, event_type, payload
            FROM task_outbox
            WHERE published = FALSE
            ORDER BY id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """
        )

        event = cursor.fetchone()

        if not event:
            connection.commit()
            return None

        payload = event["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)

        # Inject outbox_id into payload
        payload["outbox_id"] = str(event["id"])

        if event["event_type"] == "RETRY":
            target_stream = STREAM_NAME
        elif event["event_type"] == "DEAD_LETTER":
            target_stream = DEAD_LETTER_STREAM
        else:
            raise ValueError(f"Unknown outbox event type: {event['event_type']}")

        marker_key = f"scaleflow:outbox:published:{event['id']}"

        fields = []
        for key, value in payload.items():
            fields.extend([str(key), str(value)])

        # ── Task 3: Publisher Crash Simulation (Crash Point 1: Before Redis publish) ──
        crash_mode = crash_simulation or os.getenv("PUBLISHER_CRASH_TEST")
        if crash_mode == "before_redis":
            print(f"[Publisher Crash Simulation] Crashing before Redis XADD for outbox event {event['id']}...")
            connection.rollback()
            os._exit(1)
            return None

        # ── Task 1: Idempotent Outbox Publishing via Redis Lua script ──
        raw_message_id = redis_client.eval(
            OUTBOX_PUBLISH_SCRIPT,
            2,
            marker_key,
            target_stream,
            *fields
        )

        message_id = raw_message_id.decode("utf-8") if isinstance(raw_message_id, bytes) else str(raw_message_id)

        print(
            f"[Publisher {PUBLISHER_ID}] Outbox event {event['id']} published/deduplicated "
            f"with Redis message ID {message_id}"
        )

        # ── Task 3: Publisher Crash Simulation (Crash Point 2: After Redis publish, before MySQL update) ──
        if crash_mode == "after_redis":
            print(
                f"[Publisher Crash Simulation] Crashing after Redis publish, before MySQL update "
                f"for outbox event {event['id']} (Message ID: {message_id})..."
            )
            # In a crash, connection terminates and uncommitted transaction is rolled back by MySQL
            connection.rollback()
            os._exit(1)
            return None

        cursor.execute(
            """
            UPDATE task_outbox
            SET published = TRUE,
                published_at = NOW()
            WHERE id = %s
            """,
            (event["id"],)
        )

        connection.commit()

        print(
            f"[Publisher {PUBLISHER_ID}] Outbox event {event['id']} committed as published "
            f"for task {event['task_id']}"
        )

        return event["id"]

    except Exception:
        if connection:
            connection.rollback()
        raise

    finally:
        if close_cursor and cursor:
            cursor.close()
        if close_connection and connection:
            connection.close()


def publish_outbox_events(stop_event=None):
    """
    Continuous background loop that publishes pending MySQL outbox events to Redis.
    Coordinated via SELECT ... FOR UPDATE SKIP LOCKED.
    """
    print(f"[Publisher {PUBLISHER_ID}] Outbox Publisher Started")

    while stop_event is None or not stop_event.is_set():
        try:
            event_id = publish_single_outbox_event()
            if event_id is None:
                # No pending events, wait before next poll
                if stop_event:
                    if stop_event.wait(2):
                        break
                else:
                    time.sleep(2)
        except Exception as error:
            print(f"[Publisher {PUBLISHER_ID}] Outbox Publisher Error:", error)
            if stop_event:
                if stop_event.wait(3):
                    break
            else:
                time.sleep(3)


# ── Task 5: Periodic Pending-Message Recovery ──

def recover_orphaned_db_tasks():
    """
    Find tasks in MySQL stuck in PROCESSING whose lease has expired.
    Resets them to PENDING so available workers can claim and process them.
    """
    try:
        orphans = get_expired_processing_tasks()
        if orphans:
            print(f"[DB Recovery] Found {len(orphans)} orphaned task(s) with expired leases.")
            for task in orphans:
                task_id = task["id"]
                previous_worker = task.get("locked_by")
                print(f"[DB Recovery] Resetting orphaned task {task_id} (previously held by {previous_worker})")
                reset_expired_task(task_id)
                invalidate_cache(task_id)
    except Exception as error:
        print(f"[DB Recovery] Error recovering orphaned DB tasks: {error}")


def recover_pending_tasks(min_idle_time_ms=60000, batch_size=10):
    """
    Recover abandoned/pending tasks from Redis PEL using XAUTOCLAIM pagination.
    Also recovers orphaned MySQL tasks whose lease has expired.
    """
    print("[Pending Recovery] Checking for abandoned tasks from crashed workers...")
    total_recovered = 0

    try:
        start_id = "0-0"
        while True:
            result = redis_client.xautoclaim(
                STREAM_NAME,
                GROUP_NAME,
                CONSUMER_NAME,
                min_idle_time=min_idle_time_ms,
                start_id=start_id,
                count=batch_size
            )

            # redis-py xautoclaim returns (next_id, messages, [deleted_ids])
            next_id = result[0]
            messages = result[1]

            if not messages:
                break

            for message_id, data in messages:
                if isinstance(message_id, bytes):
                    message_id = message_id.decode("utf-8")

                decoded_data = {}
                for k, v in data.items():
                    k_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                    v_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                    decoded_data[k_str] = v_str

                task_id = decoded_data.get("taskId")

                print(f"\n[Pending Recovery] Recovered Abandoned Task!")
                print(f"  Message ID: {message_id}")
                print(f"  Task ID: {task_id}")

                if task_id:
                    process_task(task_id, message_id, decoded_data)
                    total_recovered += 1

            if next_id == "0-0" or next_id == start_id:
                break
            start_id = next_id

    except redis.exceptions.ResponseError as error:
        print("[Pending Recovery] Redis Recovery Error:", error)
    except Exception as error:
        print("[Pending Recovery] Unexpected Recovery Error:", error)

    # Recover orphaned MySQL tasks
    recover_orphaned_db_tasks()

    if total_recovered == 0:
        print("[Pending Recovery] No abandoned stream tasks found.")
    else:
        print(f"[Pending Recovery] Total stream tasks recovered: {total_recovered}")


def periodic_pending_recovery_worker(interval_seconds=30, stop_event=None):
    """
    Background worker that periodically triggers pending-message recovery.
    Runs every `interval_seconds` to reclaim abandoned tasks from crashed workers.
    """
    print(f"[Recovery Worker] Periodic recovery service started (interval={interval_seconds}s)")

    while True:
        try:
            recover_pending_tasks()
        except Exception as error:
            print(f"[Recovery Worker] Error in recovery cycle: {error}")

        if stop_event:
            if stop_event.wait(interval_seconds) or stop_event.is_set():
                break
        else:
            time.sleep(interval_seconds)


def consume_tasks():
    print("Waiting for tasks...")

    # Run initial recovery on startup
    recover_pending_tasks()

    while True:
        messages = redis_client.xreadgroup(
            GROUP_NAME,
            CONSUMER_NAME,
            {STREAM_NAME: ">"},
            count=1,
            block=5000
        )

        for stream, entries in messages:
            for message_id, data in entries:
                if isinstance(message_id, bytes):
                    message_id = message_id.decode("utf-8")

                decoded_data = {}
                for k, v in data.items():
                    k_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                    v_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                    decoded_data[k_str] = v_str

                task_id = decoded_data.get("taskId")

                print("\nTask Received!")
                print("Message ID:", message_id)
                print("Task ID:", task_id)

                try:
                    process_task(task_id, message_id, decoded_data)

                except Exception as error:
                    print("Worker Error:", error)
                    print("Message remains pending")


if __name__ == "__main__":
    print(f"Worker Starting (Consumer: {CONSUMER_NAME}, Publisher: {PUBLISHER_ID})...")

    connect_redis()
    create_consumer_group()

    # Task 1 & 4: Start Outbox Publisher in background
    publisher_thread = threading.Thread(
        target=publish_outbox_events,
        daemon=True
    )
    publisher_thread.start()

    # Task 5: Start Periodic Pending-Message Recovery in background
    recovery_interval = int(os.getenv("PENDING_RECOVERY_INTERVAL", "30"))
    recovery_thread = threading.Thread(
        target=periodic_pending_recovery_worker,
        args=(recovery_interval,),
        daemon=True
    )
    recovery_thread.start()

    # Start Worker Consumer (blocking main thread)
    consume_tasks()
