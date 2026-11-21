import unittest
from unittest.mock import MagicMock, patch
import sys
import os

_this_dir = os.path.dirname(os.path.abspath(__file__))
worker_dir = os.path.normpath(
    os.path.join(_this_dir, "..", "..", "api_gateway", "worker")
)
if worker_dir not in sys.path:
    sys.path.insert(0, worker_dir)

import healthcheck


class TestWorkerHealthcheck(unittest.TestCase):

    @patch("healthcheck.get_db_connection")
    @patch("healthcheck.redis_client")
    def test_healthcheck_returns_zero_when_healthy(self, mock_redis, mock_db):
        mock_redis.ping.return_value = True
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_db.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        code = healthcheck.check_health()
        self.assertEqual(code, 0)

    @patch("healthcheck.get_db_connection")
    @patch("healthcheck.redis_client")
    def test_healthcheck_returns_one_when_redis_fails(self, mock_redis, mock_db):
        mock_redis.ping.side_effect = Exception("Redis unreachable")
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_db.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        code = healthcheck.check_health()
        self.assertEqual(code, 1)

    @patch("healthcheck.get_db_connection")
    @patch("healthcheck.redis_client")
    def test_healthcheck_returns_one_when_mysql_fails(self, mock_redis, mock_db):
        mock_redis.ping.return_value = True
        mock_db.side_effect = Exception("MySQL connection refused")

        code = healthcheck.check_health()
        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
