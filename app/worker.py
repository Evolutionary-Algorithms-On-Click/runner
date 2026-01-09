import os
import time
import queue
import redis
import threading
import traceback

from app.config import (
    REDIS_URL, QUEUE_NAME, MESSAGE_RETRY_DELAY
)
from app.utils import parse_json_string
from app.storage import download_file, upload_file
from app.database import update_run_status, get_run_type
from app.streaming import redis_stream_adder
from app.executor import run_subprocess

redis_connection = None
stop_flag = threading.Event()

def process_message(body):
    """Callback function when a message is received from Redis List"""
    msg = body.strip()
    print(f"Received Message: {msg}")
    data = parse_json_string(msg)
    if data is None:
        print("Skipping. Invalid JSON message...")
        return

    runId = data.get("runId")
    fileName = data.get("fileName")
    extension = data.get("extension")
    if not all([runId, fileName, extension]):
        print("Skipping. Missing required fields in message...")
        return

    redis_adder_thread = None
    log_queue = queue.Queue()
    if REDIS_URL:
        # Start the Redis stream adder thread.
        redis_adder_thread = threading.Thread(
            target=redis_stream_adder,
            args=(REDIS_URL, log_queue, runId),
            daemon=True,
        )
        redis_adder_thread.start()
        print(f"Redis stream adder thread started for run {runId}, stream: {runId}")
    else:
        print("REDIS_URL not set. Skipping log streaming via Redis Streams.")

    local_file_path = None
    process = None
    run_status = "failed"

    try:
        # Download Code.
        local_file_path = download_file(runId, fileName, extension)
        file_parent_dir = os.path.dirname(local_file_path)
        print(f"Code downloaded to: {local_file_path}")
        print(f"Parent directory: {file_parent_dir}")

        # Update Status to 'running' & Get Type.
        runType = "scoop"  # Default fallback
        try:
            update_run_status(runId, "running")
            # We already updated status, now get type.
            # Note: The original code did both in one try/except block.
            # If update fails, we requeue.
            runType = get_run_type(runId)
        except Exception as e:
            print(f"Error updating run status/fetching type: {e}")
            print(f"Requeuing message for run {runId}...")
            if redis_connection:
                redis_connection.lpush(QUEUE_NAME, msg) # Requeue message
            time.sleep(MESSAGE_RETRY_DELAY)
            return

        # Execute code via Executor module
        run_status, process = run_subprocess(
            runId, local_file_path, file_parent_dir, runType, log_queue, redis_adder_thread
        )

        # Wait for the adder thread to process all queued logs + EOF marker.
        if redis_adder_thread:
            print("Waiting for Redis stream adder thread to finish...")
            redis_adder_thread.join(timeout=20)
            if redis_adder_thread.is_alive():
                print("Warning: Redis stream adder thread did not finish in time.")

        # Upload Results to MinIO.
        print(f"Uploading results from {file_parent_dir} for run {runId}...")
        uploaded_files = []
        if os.path.exists(file_parent_dir):
            allowed_extensions = (
                ".txt",
                ".png",
                ".gif",
                ".log",
                ".csv",
                ".json",
                ".pkl",
            )
            for file in os.listdir(file_parent_dir):
                if file.endswith(allowed_extensions):
                    file_to_upload = os.path.join(file_parent_dir, file)
                    if os.path.isfile(file_to_upload):
                        try:
                            upload_file(runId, file_to_upload)
                            uploaded_files.append(file)
                        except Exception as e:
                            print(f"Individual file upload failed for {file}: {e}")
            print(f"Uploaded files: {uploaded_files if uploaded_files else 'None'}")
        else:
            print(f"Directory {file_parent_dir} does not exist.")

        # Final Status Update.
        update_run_status(runId, run_status)

    except Exception as e:
        print(f"!! Critical error processing message for run {runId}: {e}")
        if process and process.poll() is None:
            print("Killing subprocess due to critical error.")
            process.kill()
            try:
                process.wait(timeout=5)
            except:
                pass
        run_status = "failed"
        update_run_status(runId, "failed")

    finally:
        # Final Cleanup.
        if redis_adder_thread and redis_adder_thread.is_alive():
            redis_adder_thread.join(timeout=1)

        print(f"Finished processing run {runId}. Final status: {run_status}")
        print("-" * 20)
        # TODO: Clean up local files.

# Redis Client Connection
def connect_redis():
    global redis_connection
    try:
        redis_connection = redis.from_url(REDIS_URL, decode_responses=True)
        redis_connection.ping()
        print(f"Connected to Redis: {REDIS_URL}")
    except redis.ConnectionError as conn_err:
        print(f"Failed to connect to Redis: {conn_err}")
        exit(1)

# Redis Lists Worker Loop
def worker_loop():
    print("Worker started. Waiting for messages...")
    while not stop_flag.is_set():
        try:
            item = redis_connection.brpop(QUEUE_NAME, timeout=5)
            if not item:
                continue

            _, message = item
            process_message(message)

        except redis.ConnectionError as e:
            print(f"Redis connection lost: {e}")
            connect_redis()
            time.sleep(5)
            continue
        except KeyboardInterrupt:
            print("\nStopping worker...")
            stop_flag.set()
            break
        except Exception as e:
            print(f"Unexpected error in worker loop: {e}")
            traceback.print_exc()
            time.sleep(5)

def start_worker():
    try:
        connect_redis()
        worker_loop()
    except KeyboardInterrupt:
        print("\nCleaning up and exiting...")
    finally:
        stop_flag.set()
        if redis_connection:
            redis_connection.close()
        print("Worker connection closed.")
