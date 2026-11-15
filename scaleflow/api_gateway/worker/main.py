
import redis
import os
import socket
import time
import threading

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
    complete_task
)

STREAM_NAME = "scaleflow:tasks"
GROUP_NAME = "scaleflow-workers"
CONSUMER_NAME = os.getenv("WORKER_ID") or socket.gethostname()
DEAD_LETTER_STREAM = "scaleflow:tasks:dead"


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
    redis_client.delete(f"task:{task_id}")
    redis_client.delete("tasks:all")


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

def process_task(task_id, message_id):
    """
    Single unified task processor.

    Flow:
    
    1. Check if task exists and is not already COMPLETED
    2. Atomically claim the task (MySQL lock)
    3. Crash simulation check (CRASH_TEST=true)
    4. Execute the task
    5. Mark COMPLETED and release lock
    6. ACK the Redis message

    On failure:
    - Retry if retry_count < max_retries
    - Move to Dead Letter Stream if max retries exceeded
    - ACK original message only after retry/DLQ publish succeeds
    """
    worker_id = CONSUMER_NAME

    # ── Step 1: Fetch task and validate ──
    task = get_task_details(task_id)

    if not task:
        print(f"Task {task_id} not found in database. Acknowledging stale message.")
        redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
        return

    if task["status"] == "COMPLETED":
        print(f"Task {task_id} already COMPLETED. Skipping duplicate.")
        redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
        return

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
        return  # Do NOT ack — let the lock holder process it

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

        redis_client.xack(
            STREAM_NAME,
            GROUP_NAME,
            message_id
        )

        print("Original message acknowledged")

    finally:
        heartbeat.stop()


def publish_outbox_events():
    """Publish pending MySQL outbox events to Redis."""
    print("Outbox Publisher Started")

    while True:
        connection = None
        cursor = None

        try:
            connection = get_db_connection()
            cursor = connection.cursor(dictionary=True)

            # Lock one pending event to prevent concurrent publishers
            cursor.execute(
                """
                SELECT id, task_id, event_type, payload
                FROM task_outbox
                WHERE published = FALSE
                ORDER BY id
                LIMIT 1
                FOR UPDATE
                """
            )

            event = cursor.fetchone()

            if not event:
                connection.commit()
                time.sleep(2)
                continue

            payload = event["payload"]

            if isinstance(payload, str):
                import json
                payload = json.loads(payload)

            payload["outbox_id"] = str(event["id"])

            if event["event_type"] == "RETRY":
                redis_client.xadd(STREAM_NAME, payload)

            elif event["event_type"] == "DEAD_LETTER":
                redis_client.xadd(DEAD_LETTER_STREAM, payload)

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
                f"Outbox event {event['id']} published "
                f"for task {event['task_id']}"
            )

        except Exception as error:
            if connection:
                connection.rollback()

            print("Outbox Publisher Error:", error)
            time.sleep(3)

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()


def recover_pending_tasks():
    """
    Use XAUTOCLAIM to recover tasks from crashed workers.
    Claims messages that have been pending for > 60 seconds (min_idle_time).
    The corresponding MySQL claim_task() will also check lease_expires_at.
    """
    print("Checking for abandoned tasks from crashed workers...")

    try:
        next_id, messages, deleted_ids = redis_client.xautoclaim(
            STREAM_NAME,
            GROUP_NAME,
            CONSUMER_NAME,
            min_idle_time=60000,
            start_id="0-0",
            count=5
        )

        if not messages:
            print("No abandoned tasks found.")
            return

        for message_id, data in messages:
            task_id = data.get("taskId")

            print(f"\nRecovered Pending Task!")
            print(f"  Message ID: {message_id}")
            print(f"  Task ID: {task_id}")

            process_task(task_id, message_id)

    except redis.exceptions.ResponseError as error:
        print("Pending Recovery Error:", error)


def consume_tasks():
    print("Waiting for tasks...")

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

                task_id = data.get("taskId")

                print("\nTask Received!")
                print("Message ID:", message_id)
                print("Task ID:", task_id)

                try:
                    process_task(task_id, message_id)

                except Exception as error:
                    print("Worker Error:", error)
                    print("Message remains pending")




if __name__ == "__main__":
    print("Worker Starting...")

    connect_redis()
    create_consumer_group()

    # Start Outbox Publisher in background
    publisher_thread = threading.Thread(
        target=publish_outbox_events,
        daemon=True
    )
    publisher_thread.start()

    # Start Worker Consumer
    consume_tasks()
