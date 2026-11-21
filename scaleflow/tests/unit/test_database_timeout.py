import os
import sys
import unittest
from unittest.mock import patch, MagicMock

# Ensure worker package is in sys.path
WORKER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "api_gateway", "worker"))
if WORKER_DIR not in sys.path:
    sys.path.insert(0, WORKER_DIR)

import database


class TestDatabaseConnectionTimeout(unittest.TestCase):

    def setUp(self):
        # Reset cached connection pool before each test
        database._connection_pool = None

    def tearDown(self):
        database._connection_pool = None

    @patch("database.get_connection_pool", return_value=None)
    @patch("database.mysql.connector.connect")
    def test_direct_connection_uses_timeout_from_env(self, mock_connect, mock_pool):
        """Verify connection_timeout parameter is passed to mysql.connector.connect."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with patch.dict(os.environ, {"DB_CONNECT_TIMEOUT": "7"}):
            conn = database.get_db_connection(max_retries=1)
            self.assertEqual(conn, mock_conn)
            mock_connect.assert_called_once()
            _, kwargs = mock_connect.call_args
            self.assertEqual(kwargs.get("connection_timeout"), 7)

    @patch("database.get_connection_pool", return_value=None)
    @patch("database.time.sleep")
    @patch("database.mysql.connector.connect")
    def test_connection_retries_and_succeeds(self, mock_connect, mock_sleep, mock_pool):
        """Verify get_db_connection retries with backoff and succeeds on subsequent attempt."""
        import mysql.connector
        mock_conn = MagicMock()
        # Fail on attempt 1, succeed on attempt 2
        mock_connect.side_effect = [
            mysql.connector.Error("Connection timed out"),
            mock_conn
        ]

        conn = database.get_db_connection(max_retries=3, retry_delay=0.01)
        self.assertEqual(conn, mock_conn)
        self.assertEqual(mock_connect.call_count, 2)
        mock_sleep.assert_called_once()

    @patch("database.get_connection_pool", return_value=None)
    @patch("database.time.sleep")
    @patch("database.mysql.connector.connect")
    def test_connection_exhausts_retries_and_raises(self, mock_connect, mock_sleep, mock_pool):
        """Verify get_db_connection raises after exceeding max_retries."""
        import mysql.connector
        mock_connect.side_effect = mysql.connector.Error("Can't connect to MySQL server")

        with self.assertRaises(mysql.connector.Error):
            database.get_db_connection(max_retries=2, retry_delay=0.01)

        self.assertEqual(mock_connect.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)

    @patch("database.pooling.MySQLConnectionPool")
    def test_pool_connection_success(self, mock_pool_cls):
        """Verify get_db_connection acquires from pool when pool is active."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.get_connection.return_value = mock_conn
        mock_pool_cls.return_value = mock_pool

        conn = database.get_db_connection(max_retries=1)
        self.assertEqual(conn, mock_conn)
        mock_pool.get_connection.assert_called_once()


if __name__ == "__main__":
    unittest.main()
