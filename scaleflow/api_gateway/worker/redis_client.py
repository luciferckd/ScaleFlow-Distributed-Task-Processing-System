import os
import sys
import redis
from redis.retry import Retry
from redis.backoff import ExponentialBackoff
from dotenv import load_dotenv

load_dotenv()

_worker_dir = os.path.dirname(os.path.abspath(__file__))
if _worker_dir not in sys.path:
    sys.path.insert(0, _worker_dir)

# Configure retry with exponential backoff (TimeoutError is included by default in redis-py 6+)
retry_strategy = Retry(ExponentialBackoff(), retries=int(os.getenv("REDIS_MAX_RETRIES", "3")))

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True,
    socket_timeout=10,
    socket_connect_timeout=5,
    retry=retry_strategy
)

def connect_redis():
    try:
        redis_client.ping()
        print("Redis Connected Successfully")
    
    except redis.RedisError as error:
        print(f"Redis Connection Failed: {error}")
        raise