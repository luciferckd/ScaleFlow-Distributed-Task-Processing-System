import redis
import os
from dotenv import load_dotenv

load_dotenv()

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True,
    socket_timeout=10,
    socket_connect_timeout=5,
    retry_on_timeout=True
)

def connect_redis():
    try:
        redis_client.ping()
        print("Redis Connected Successfully")
    
    except redis.RedisError as error:
        print(f"Redis Connection Failed: {error}")
        raise