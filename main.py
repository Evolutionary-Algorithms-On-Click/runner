import os
import subprocess
import json
from minio import Minio
from minio.error import S3Error
import psycopg2
import threading
import queue
import redis
import traceback
import time

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
STREAM_END = object()
LOG_DATA_FIELD = b"log_data"

redis_connection = None
stop_flag = threading.Event()

def parse_json_string(json_string):
    """Parses a JSON string and returns a Python object."""
    try:
        data = json.loads(json_string)
        return data
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None


def download_file(run_id, file_name, extension):
    """Downloads a file from MinIO storage."""
    BUCKET_NAME = "code"
    try:
        minio_client = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        object_name = f"{run_id}/{file_name}.{extension}"
        download_path = f"code/{run_id}/{file_name}.{extension}"
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        minio_client.fget_object(BUCKET_NAME, object_name, download_path)
        print(f"Successfully downloaded {object_name} to {download_path}")
        return os.path.abspath(download_path)
    except S3Error as exc:
        print(f"Failed to download {object_name}: {exc}")
        raise exc
    except Exception as e:
        print(f"An unexpected error occurred during download: {e}")
        raise e


def upload_file(run_id, file_path):
    """Uploads a file to MinIO storage."""
    BUCKET_NAME = "code"
    try:
        minio_client = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        # Extract filename from the full path.
        file_name_with_ext = os.path.basename(file_path)
        object_name = f"{run_id}/{file_name_with_ext}"
        minio_client.fput_object(BUCKET_NAME, object_name, file_path)
        print(f"Successfully uploaded {file_path} as {object_name}")
    except S3Error as exc:
        print(f"Failed to upload {file_path}: {exc}")
        raise exc
    except Exception as e:
        print(f"An unexpected error occurred during upload: {e}")
        raise e


def stream_reader(stream, stream_name, log_queue):
    """Reads lines from a stream and puts them onto the queue."""
    try:
        for line in iter(stream.readline, b""):
            try:
                decoded_line = line.decode("utf-8").rstrip()
                log_entry = json.dumps({"stream": stream_name, "line": decoded_line})
                # print(f"Read log: {log_entry}")
                log_queue.put(log_entry)
            except UnicodeDecodeError:
                log_entry = json.dumps(
                    {"stream": stream_name, "line": repr(line)[2:-1]}
                )
                log_queue.put(log_entry)
            except Exception as e:
                print(f"Error processing log line from {stream_name}: {e}")
        print(f"Stream reader for {stream_name} finished.")
    except Exception as e:
        print(f"Error in stream reader for {stream_name}: {e}")
    finally:
        log_queue.put(STREAM_END)
        print(f"Stream reader for {stream_name} exiting (sent STREAM_END).")
        if stream:
            stream.close()


def redis_stream_adder(redis_url, log_queue, run_id):
    """Connects to Redis and adds logs from the queue to a run-specific Redis Stream."""
    r = None
    active_streams = 2  # (stdout, stderr)
    connection_attempts = 0
    max_attempts = 5
    retry_delay = 2
    stream_name = run_id  # Use run_id as the stream name
    srream_ttl_seconds = 120

    # Redis Connection Loop.
    while connection_attempts < max_attempts:
        try:
            print(f"Attempting to connect to Redis for Stream: {redis_url}")
            r = redis.from_url(redis_url, decode_responses=False)
            r.ping()
            print(f"Redis Stream connection established to {redis_url}")
            break
        except redis.exceptions.ConnectionError as e:
            connection_attempts += 1
            print(
                f"Redis Stream connection failed (Attempt {connection_attempts}/{max_attempts}): {e}"
            )
            if connection_attempts >= max_attempts:
                print(
                    "Max connection attempts reached. Cannot add logs to Redis Stream."
                )
                # Drain queue.
                while active_streams > 0:
                    item = log_queue.get()
                    if item is STREAM_END:
                        active_streams -= 1
                    log_queue.task_done()
                return
            print(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay *= 2
        except Exception as e:
            print(f"Unexpected error during Redis Stream connection: {e}")
            # Drain queue.
            while active_streams > 0:
                item = log_queue.get()
                if item is STREAM_END:
                    active_streams -= 1
                log_queue.task_done()
            return

    # Log Adding Loop.
    entries_added = 0
    try:
        while active_streams > 0:
            try:
                log_entry = log_queue.get(timeout=300)
            except queue.Empty:
                print(
                    "Log queue empty timeout reached. Checking Redis stream connection."
                )
                try:
                    if r:
                        r.ping()
                    else:
                        break
                except redis.exceptions.ConnectionError:
                    print("Redis stream connection lost unexpectedly.")
                    break
                continue

            if log_entry is STREAM_END:
                active_streams -= 1
                print(
                    f"Received stream end signal. Active streams remaining: {active_streams}"
                )
            elif isinstance(log_entry, str):
                try:
                    if r:
                        payload = {LOG_DATA_FIELD: log_entry}
                        # print(f"DEBUG: XADD to stream='{stream_name}' payload='{payload}'") # Debug.
                        entry_id = r.xadd(stream_name, payload)
                        r.expire(stream_name, srream_ttl_seconds)
                        entries_added += 1
                        # print(f"Added to Redis Stream '{stream_name}', ID: {entry_id.decode()}") # Debug.
                    else:
                        print("Cannot add log to stream, Redis is not connected.")
                except redis.exceptions.RedisError as e:
                    print(f"Error using XADD for Redis Stream '{stream_name}': {e}")
                except Exception as e:
                    print(f"Unexpected error during XADD: {e}")
                    break

            log_queue.task_done()

        # After both streams end
        if r:
            # Add an End-Of-File marker message to the stream.
            eof_message = json.dumps({"status": "EOF", "runId": run_id})
            eof_payload = {LOG_DATA_FIELD: eof_message}
            try:
                entry_id = r.xadd(stream_name, eof_payload)
                r.expire(stream_name, srream_ttl_seconds)
                print(
                    f"Added EOF marker to Redis Stream '{stream_name}', ID: {entry_id.decode()}"
                )
                entries_added += 1
            except redis.exceptions.RedisError as e:
                print(f"Error adding EOF marker to Redis Stream: {e}")

    except Exception as e:
        print(f"Error in Redis stream adder loop: {e}")
    finally:
        print(
            f"Redis stream adder thread finishing. Total entries added: {entries_added}"
        )
        if r:
            try:
                r.close()
                print("Redis stream connection closed.")
            except Exception as e:
                print(f"Error closing Redis stream connection: {e}")


# Callback function that is called when a message is received.
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
    stdout_thread = None
    stderr_thread = None
    run_status = "failed"
    conn = None

    try:
        # Download Code.
        local_file_path = download_file(runId, fileName, extension)
        file_parent_dir = os.path.dirname(local_file_path)
        print(f"Code downloaded to: {local_file_path}")
        print(f"Parent directory: {file_parent_dir}")

        # Update Status to 'running' & Get Type.
        runType = "scoop"
        try:
            conn = psycopg2.connect(COCKROACHDB_URL)
            cur = conn.cursor()
            cur.execute("UPDATE run SET status = 'running' WHERE id = %s", (runId,))
            cur.execute("SELECT type FROM run WHERE id = %s", (runId,))
            result = cur.fetchone()
            if result:
                runType = result[0]
                print(f"Run type: {runType}")
            else:
                print(f"Warning: Could not fetch run type for {runId}")
            conn.commit()
            print(f"Run {runId} status updated to 'running'.")
            run_status = "running"
        except psycopg2.Error as e:
            print(f"Error updating run status/fetching type in CockroachDB: {e}")
            run_status = "failed"
            print(f"Requeuing message for run {runId}...")
            redis_connection.lpush(QUEUE_NAME, msg) # Requeue message
            time.sleep(MESSAGE_RETRY_DELAY)
            return
        finally:
            if conn:
                if "cur" in locals() and cur:
                    cur.close()
                conn.close()

        # Execute code.
        command = []
        if runType == "ml":
            command = ["python", local_file_path]
        else:
            command = ["python", "-m", "scoop", local_file_path]
        timeout_sec = 3600
        print(f"Running command: {' '.join(command)} in {file_parent_dir}")
        print(f"Timeout: {timeout_sec} seconds")

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=file_parent_dir,
        )

        # Start reader threads if Redis is enabled.
        if redis_adder_thread:
            stdout_thread = threading.Thread(
                target=stream_reader,
                args=(process.stdout, "stdout", log_queue),
                daemon=True,
            )
            stderr_thread = threading.Thread(
                target=stream_reader,
                args=(process.stderr, "stderr", log_queue),
                daemon=True,
            )
            stdout_thread.start()
            stderr_thread.start()
        else:
            process.stdout.read()
            process.stderr.read()
            print("Redis not configured. Subprocess output will not be streamed.")

        # Wait for Process Completion.
        try:
            print(f"Waiting for subprocess (PID: {process.pid}) to complete...")
            return_code = process.wait(timeout=timeout_sec)
            print(f"Subprocess finished with return code: {return_code}")
            if return_code == 0:
                if any(
                    file.endswith(".txt")
                    for file in os.listdir(file_parent_dir)
                    if os.path.isfile(os.path.join(file_parent_dir, file))
                ):
                    run_status = "completed"
                else:
                    print(
                        "Warning: Process exited with code 0 but expected output files not found."
                    )
                    run_status = "completed"
            else:
                run_status = "failed"
        except subprocess.TimeoutExpired:
            print(f"Subprocess timed out after {timeout_sec} seconds. Terminating...")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            run_status = "timed_out"
            print("Subprocess terminated due to timeout.")
        except Exception as e:
            print(f"Error waiting for subprocess: {e}")
            run_status = "failed"
            if process.poll() is None:
                process.kill()
                process.wait()

        # Wait for Log Streaming to Finish.
        if stdout_thread:
            stdout_thread.join(timeout=10)
            if stdout_thread.is_alive():
                print("Warning: stdout reader thread did not finish.")
        if stderr_thread:
            stderr_thread.join(timeout=10)
            if stderr_thread.is_alive():
                print("Warning: stderr reader thread did not finish.")

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
        try:
            conn = psycopg2.connect(COCKROACHDB_URL)
            cur = conn.cursor()
            print(f"Updating run {runId} final status to '{run_status}'.")
            cur.execute("UPDATE run SET status = %s WHERE id = %s", (run_status, runId))
            conn.commit()
        except psycopg2.Error as e:
            print(f"Error updating final run status in CockroachDB: {e}")
        finally:
            if conn:
                if "cur" in locals() and cur:
                    cur.close()
                conn.close()

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
        try:
            conn = psycopg2.connect(COCKROACHDB_URL)
            cur = conn.cursor()
            print(f"Updating run {runId} status to 'failed' due to critical error.")
            cur.execute("UPDATE run SET status = 'failed' WHERE id = %s", (runId,))
            conn.commit()
        except psycopg2.Error as db_e:
            print(f"Error updating run status to failed in CockroachDB: {db_e}")
        finally:
            if conn:
                if "cur" in locals() and cur:
                    cur.close()
                conn.close()

    finally:
        # Final Cleanup.
        if stdout_thread and stdout_thread.is_alive():
            stdout_thread.join(timeout=1)
        if stderr_thread and stderr_thread.is_alive():
            stderr_thread.join(timeout=1)
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