import json
import time
import queue
import redis
import threading
from app.config import LOG_DATA_FIELD

STREAM_END = object()

def stream_reader(stream, stream_name, log_queue):
    """Reads lines from a stream and puts them onto the queue."""
    try:
        for line in iter(stream.readline, b""):
            try:
                decoded_line = line.decode("utf-8").rstrip()
                log_entry = json.dumps({"stream": stream_name, "line": decoded_line})
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
                        entry_id = r.xadd(stream_name, payload)
                        r.expire(stream_name, srream_ttl_seconds)
                        entries_added += 1
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
