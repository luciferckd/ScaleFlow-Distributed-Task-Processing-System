
import os
import sys

_worker_dir = os.path.dirname(os.path.abspath(__file__))
if _worker_dir not in sys.path:
    sys.path.insert(0, _worker_dir)

from redis_client import connect_redis

if __name__ == "__main__":
    connect_redis()

