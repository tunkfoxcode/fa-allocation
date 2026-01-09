"""
RQ Worker for processing background jobs

Usage:
    python worker.py
"""
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from redis import Redis
from rq import Worker, Queue, Connection
from app_config import get_settings

settings = get_settings()

if __name__ == '__main__':
    # Connect to Redis
    redis_conn = Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB
    )
    
    # Create worker
    with Connection(redis_conn):
        worker = Worker(['report_build'])
        print(f"[INFO] Starting RQ worker for queue: report_build")
        print(f"[INFO] Redis: {settings.REDIS_HOST}:{settings.REDIS_PORT}")
        print(f"[INFO] Worker will process jobs one at a time")
        print("-" * 50)
        worker.work()
