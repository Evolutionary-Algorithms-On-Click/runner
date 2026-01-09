import os
import threading
import subprocess
from app.config import REDIS_URL
from app.streaming import stream_reader, redis_stream_adder

def run_subprocess(run_id, local_file_path, file_parent_dir, run_type, log_queue, redis_adder_thread):
    """Executes the code in a subprocess and manages streams."""
    process = None
    stdout_thread = None
    stderr_thread = None
    run_status = "failed"
    
    command = []
    if run_type == "ml":
        command = ["python", local_file_path]
    else:
        command = ["python", "-m", "scoop", local_file_path]
    timeout_sec = 3600
    print(f"Running command: {' '.join(command)} in {file_parent_dir}")
    print(f"Timeout: {timeout_sec} seconds")

    try:
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
        if process and process.poll() is None:
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
            
    return run_status, process
