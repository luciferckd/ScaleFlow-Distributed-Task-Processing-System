
import os
import json
import mysql.connector
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            port=int(os.getenv("DB_PORT", 3306))
        )

        print("MySQL Connected Successfully")
        return connection

    except mysql.connector.Error as error:
        print(f"MySQL Connection Failed: {error}")
        raise


def get_task_details(task_id):
    """Fetch full task details including lock columns."""
    connection = get_db_connection()
    cursor = None

    try:
        cursor = connection.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT id, user_id, title, payload, status,
                   priority, retry_count, max_retries,
                   created_at, updated_at,
                   locked_by, lease_expires_at
            FROM tasks
            WHERE id = %s
            """,
            (task_id,)
        )

        task = cursor.fetchone()
        return task

    finally:
        if cursor:
            cursor.close()
        connection.close()


def claim_task(task_id, worker_id, lease_seconds=60):
    """
    Atomically claim a task using MySQL.
    Only claims if:
      - status is PENDING, OR
      - status is PROCESSING but the lease has expired (crashed worker)
    Returns True if this worker successfully claimed the task.
    """
    connection = get_db_connection()
    cursor = None

    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            UPDATE tasks
            SET
                status = 'PROCESSING',
                locked_by = %s,
                lease_expires_at = DATE_ADD(
                    NOW(),
                    INTERVAL %s SECOND
                )
            WHERE id = %s
              AND (
                    status = 'PENDING'
                    OR (
                        status = 'PROCESSING'
                        AND lease_expires_at < NOW()
                    )
              )
            """,
            (worker_id, lease_seconds, task_id)
        )

        connection.commit()

        return cursor.rowcount == 1

    finally:
        if cursor:
            cursor.close()
        connection.close()


def complete_task(task_id, worker_id, retry_count=0):
    """
    Atomically mark a task as COMPLETED and release its lock in a single query.
    Only succeeds if the task is currently PROCESSING and locked by this worker.
    """
    connection = get_db_connection()
    cursor = None

    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            UPDATE tasks
            SET status = 'COMPLETED',
                retry_count = %s,
                locked_by = NULL,
                lease_expires_at = NULL
            WHERE id = %s
              AND locked_by = %s
              AND status = 'PROCESSING'
            """,
            (retry_count, task_id, worker_id)
        )

        connection.commit()

        return cursor.rowcount == 1

    finally:
        if cursor:
            cursor.close()
        connection.close()


def update_task_status(task_id, worker_id, retry_count, status):
    """
    Update task status and retry_count.
    Only the worker holding the lock (locked_by = worker_id) can update.
    This prevents a recovered task from being overwritten by a stale worker.
    """
    connection = get_db_connection()
    cursor = None

    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            UPDATE tasks
            SET retry_count = %s,
                status = %s
            WHERE id = %s
              AND locked_by = %s
            """,
            (retry_count, status, task_id, worker_id)
        )

        connection.commit()

        if cursor.rowcount == 0:
            print(f"WARNING: Task {task_id} update skipped — lock not held by {worker_id}")
            return False

        return True

    finally:
        if cursor:
            cursor.close()
        connection.close()


def release_task_lock(task_id, worker_id):
    """
    Release the lock after task completion or failure.
    Only the current lock holder can release.
    """
    connection = get_db_connection()
    cursor = None

    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            UPDATE tasks
            SET locked_by = NULL,
                lease_expires_at = NULL
            WHERE id = %s
              AND locked_by = %s
            """,
            (task_id, worker_id)
        )

        connection.commit()

        if cursor.rowcount == 1:
            print(f"Lock released for task {task_id}")
        else:
            print(f"WARNING: Lock release skipped — task {task_id} not held by {worker_id}")

    finally:
        if cursor:
            cursor.close()
        connection.close()


def renew_task_lease(task_id, worker_id, extend_seconds=60):
    connection = get_db_connection()
    cursor = None

    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            UPDATE tasks
            SET lease_expires_at = DATE_ADD(
                NOW(),
                INTERVAL %s SECOND
            )
            WHERE id = %s
              AND locked_by = %s
              AND status = 'PROCESSING'
              AND lease_expires_at > NOW()
            """,
            (extend_seconds, task_id, worker_id)
        )

        connection.commit()

        renewed = cursor.rowcount == 1

        if renewed:
            print(f"Heartbeat successful for task {task_id}")
        else:
            print(f"Heartbeat failed: lease lost for task {task_id}")

        return renewed

    finally:
        if cursor:
            cursor.close()
        connection.close()



def update_retry_status(task_id, worker_id, retry_count, status):
    connection = get_db_connection()
    cursor = None

    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            UPDATE tasks
            SET retry_count = %s,
                status = %s,
                locked_by = NULL,
                lease_expires_at = NULL
            WHERE id = %s
              AND status = 'PROCESSING'
              AND locked_by = %s
              AND lease_expires_at > NOW()
            """,
            (retry_count, status, task_id, worker_id)
        )

        connection.commit()

        return cursor.rowcount == 1

    finally:
        if cursor:
            cursor.close()
        connection.close()


def update_retry_status_with_outbox(
    task_id,
    worker_id,
    retry_count,
    status,
    error
):
    connection = get_db_connection()
    cursor = None

    try:
        cursor = connection.cursor()

        event_type = (
            "RETRY" if status == "PENDING"
            else "DEAD_LETTER"
        )

        payload = {
            "taskId": str(task_id),
            "retry_count": str(retry_count),
            "error": str(error)
        }

        # Step 1: Update task and release lock
        cursor.execute(
            """
            UPDATE tasks
            SET retry_count = %s,
                status = %s,
                locked_by = NULL,
                lease_expires_at = NULL
            WHERE id = %s
              AND status = 'PROCESSING'
              AND locked_by = %s
              AND lease_expires_at > NOW()
            """,
            (retry_count, status, task_id, worker_id)
        )

        if cursor.rowcount != 1:
            connection.rollback()
            return False

        # Step 2: Insert outbox event in same transaction
        cursor.execute(
            """
            INSERT INTO task_outbox
                (task_id, event_type, payload)
            VALUES (%s, %s, %s)
            """,
            (task_id, event_type, json.dumps(payload))
        )

        connection.commit()
        return True

    except Exception:
        connection.rollback()
        raise

    finally:
        if cursor:
            cursor.close()
        connection.close()
