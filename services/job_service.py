import json
from flask import g


def create_job(job_id):
    g.db.execute_query(
        "INSERT INTO jobs (id, status, pct, log) VALUES (?, ?, ?, ?)",
        (job_id, "running", 0, json.dumps([])),
    )
    g.db.commit()


def update_job(job_id, pct=None, message=None, error=None, result=None, done=False):
    """
    Update job progress and status.

    Args:
        job_id: Job identifier
        pct: Progress percentage (0-100)
        message: Log message to append
        error: Error message if job failed
        result: Result data (will be JSON serialized)
        done: Whether job is completed successfully
    """
    # Get existing logs
    row = g.db.execute_query("SELECT log FROM jobs WHERE id=?", (job_id,)).fetchone()
    logs = json.loads(row["log"]) if row else []

    # Append new log message if provided
    if message:
        logs.append(message)

    # Determine status
    if error:
        status = "failed"
    elif done:
        status = "completed"
    else:
        status = "running"

    # Serialize result if provided
    result_json = json.dumps(result) if result is not None else None

    # Update database
    g.db.execute_query(
        "UPDATE jobs SET pct=?, log=?, error=?, result=?, status=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
        (pct or 0, json.dumps(logs), error, result_json, status, job_id),
    )
    g.db.commit()


def get_job(job_id):
    """
    Retrieve job status and data.

    Returns dict with keys: pct, log, done, error, result, status
    """
    row = g.db.execute_query("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not row:
        return None

    # Deserialize result if present
    result = None
    if row["result"]:
        try:
            result = json.loads(row["result"])
        except (json.JSONDecodeError, TypeError):
            result = row["result"]  # fallback to raw value

    return {
        "pct": row["pct"] or 0,
        "log": json.loads(row["log"] or "[]"),
        "done": row["status"] in ("completed", "failed"),
        "error": row["error"],
        "result": result,
        "status": row["status"],  # include raw status for debugging
    }