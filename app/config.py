import os

# Read environment variables.
QUEUE_NAME = os.getenv("REDIS_QUEUE_NAME", "task_queue")
MINIO_URL = os.getenv("MINIO_URL", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
COCKROACHDB_URL = os.getenv(
    "COCKROACHDB_URL", "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MESSAGE_RETRY_DELAY = 10
LOG_DATA_FIELD = b"log_data"
