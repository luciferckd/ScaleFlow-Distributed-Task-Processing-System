"""
ScaleFlow Worker Health Check Script
Used by Docker / Kubernetes container probes to monitor worker health.

Exit codes:
  0: Healthy (MySQL & Redis connected)
  1: Unhealthy (Connection failure)
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from redis_client import redis_client
from database import get_db_connection


def check_health():
    is_healthy = True

    # 1. Check Redis
    try:
        redis_client.ping()
        print("Redis Health: OK")
    except Exception as error:
        print(f"Redis Health FAILED: {error}", file=sys.stderr)
        is_healthy = False

    # 2. Check MySQL
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        print("MySQL Health: OK")
    except Exception as error:
        print(f"MySQL Health FAILED: {error}", file=sys.stderr)
        is_healthy = False

    return 0 if is_healthy else 1


if __name__ == "__main__":
    exit_code = check_health()
    sys.exit(exit_code)
