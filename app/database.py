import psycopg2
from app.config import COCKROACHDB_URL

def update_run_status(run_id, status):
    """Updates the status of a run in the database."""
    conn = None
    try:
        conn = psycopg2.connect(COCKROACHDB_URL)
        cur = conn.cursor()
        print(f"Updating run {run_id} status to '{status}'.")
        cur.execute("UPDATE run SET status = %s WHERE id = %s", (status, run_id))
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error updating run status in CockroachDB: {e}")
    finally:
        if conn:
            if "cur" in locals() and cur:
                cur.close()
            conn.close()

def get_run_type(run_id):
    """Fetches the type of the run from the database."""
    conn = None
    run_type = "scoop"  # Default
    try:
        conn = psycopg2.connect(COCKROACHDB_URL)
        cur = conn.cursor()
        cur.execute("SELECT type FROM run WHERE id = %s", (run_id,))
        result = cur.fetchone()
        if result:
            run_type = result[0]
            print(f"Run type: {run_type}")
        else:
            print(f"Warning: Could not fetch run type for {run_id}")
    except psycopg2.Error as e:
        print(f"Error fetching run type in CockroachDB: {e}")
        raise e  # Re-raise to handle upstream
    finally:
        if conn:
            if "cur" in locals() and cur:
                cur.close()
            conn.close()
    return run_type
