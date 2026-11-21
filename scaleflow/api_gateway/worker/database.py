
import os
import sys
import json
import time

# Ensure worker directory is in sys.path for local module resolution
_worker_dir = os.path.dirname(os.path.abspath(__file__))
if _worker_dir not in sys.path:
    sys.path.insert(0, _worker_dir)

import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv
from logger import logger

load_dotenv()

_connection_pool = None


def get_db_config():
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_NAME", "scaleflow"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "connection_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "5")),
    }


def get_connection_pool():
    global _connection_pool
    if _connection_pool is None:
        try:
            config = get_db_config()
            pool_size = int(os.getenv("DB_POOL_SIZE", "5"))
            _connection_pool = pooling.MySQLConnectionPool(
                pool_name="scaleflow_worker_pool",
                pool_size=pool_size,
                **config
            )
            logger.info("Initialized MySQL connection pool", extra={"event": "DB_POOL_INIT"})
        except Exception as e:
            logger.warning(
                f"MySQL connection pool creation skipped/failed: {e}. Falling back to direct connections.",
                extra={"event": "DB_POOL_FALLBACK", "error": str(e)}
            )
            _connection_pool = None
    return _connection_pool


def get_db_connection(max_retries=3, retry_delay=0.1):
    """
    Acquires a database connection with timeout handling and retry backoff.
    Attempts pool acquisition first, falling back to direct connection with connection_timeout.
    """
    last_error = None
    max_retries = int(os.getenv("DB_MAX_RETRIES", max_retries))
    config = get_db_config()

    for attempt in range(1, max_retries + 1):
        try:
            pool = get_connection_pool()
            if pool:
                try:
                    conn = pool.get_connection()
                    return conn
                except Exception as pool_err:
                    logger.warning(
                        f"Pool connection failed: {pool_err}. Falling back to direct connection.",
                        extra={"event": "DB_POOL_GET_FAILED", "error": str(pool_err)}
                    )

            # Direct connection with connection_timeout
            connection = mysql.connector.connect(**config)
            return connection

        except (mysql.connector.Error, Exception) as error:
            last_error = error
            logger.warning(
                f"MySQL connection attempt {attempt}/{max_retries} failed: {error}",
                extra={"event": "DB_CONNECT_RETRY", "retry_count": attempt, "error": str(error)}
            )
            if attempt < max_retries:
                time.sleep(retry_delay * attempt)

    logger.error(
        f"MySQL Connection Failed after {max_retries} attempts: {last_error}",
        extra={"event": "DB_CONNECT_FAILED", "error": str(last_error)}
    )
    raise last_error


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


def get_expired_processing_tasks():
    """
    Fetch tasks that are stuck in PROCESSING status whose lease has expired.
    Used by periodic recovery to identify orphaned tasks from crashed workers.
    """
    connection = get_db_connection()
    cursor = None

    try:
        cursor = connection.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT id, user_id, title, payload, status,
                   retry_count, max_retries, locked_by, lease_expires_at
            FROM tasks
            WHERE status = 'PROCESSING'
              AND lease_expires_at < NOW()
            """
        )

        return cursor.fetchall()

    finally:
        if cursor:
            cursor.close()
        connection.close()


def reset_expired_task(task_id):
    """
    Reset an orphaned PROCESSING task whose lease expired back to PENDING.
    """
    connection = get_db_connection()
    cursor = None

    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            UPDATE tasks
            SET status = 'PENDING',
                locked_by = NULL,
                lease_expires_at = NULL
            WHERE id = %s
              AND status = 'PROCESSING'
              AND lease_expires_at < NOW()
            """,
            (task_id,)
        )

        connection.commit()
        return cursor.rowcount == 1

    finally:
        if cursor:
            cursor.close()
        connection.close()
