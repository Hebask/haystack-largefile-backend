import json
from app.db import get_conn
from app.utils.ids import new_id


def create_index_job(file_id: str, user_id: str):
    job_id = new_id()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO jobs (id, file_id, user_id, job_type, status, progress, message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (job_id, file_id, user_id, "index_file", "pending", 0, "Job created"),
    )

    cur.execute(
        """
        INSERT OR REPLACE INTO job_checkpoints
        (job_id, file_id, user_id, last_completed_page, total_pages, chunk_ids_json, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (job_id, file_id, user_id, 0, 0, json.dumps([])),
    )

    cur.execute(
        """
        INSERT OR REPLACE INTO job_cancellations (job_id, cancel_requested, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        """,
        (job_id, 0),
    )

    conn.commit()
    conn.close()
    return job_id


def list_jobs(user_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM jobs WHERE user_id = ? ORDER BY created_at DESC",
        (user_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_job(job_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def update_job(job_id: str, *, status=None, progress=None, message=None, error=None):
    conn = get_conn()
    cur = conn.cursor()

    fields = []
    values = []

    if status is not None:
        fields.append("status = ?")
        values.append(status)
    if progress is not None:
        fields.append("progress = ?")
        values.append(progress)
    if message is not None:
        fields.append("message = ?")
        values.append(message)
    if error is not None:
        fields.append("error = ?")
        values.append(error)

    fields.append("updated_at = CURRENT_TIMESTAMP")
    values.append(job_id)

    cur.execute(f"UPDATE jobs SET {', '.join(fields)} WHERE id = ?", values)
    conn.commit()
    conn.close()


def set_file_status(file_id: str, status: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE files SET status = ? WHERE id = ?", (status, file_id))
    conn.commit()
    conn.close()


def get_file_record(file_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM files WHERE id = ?", (file_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def list_indexed_files(user_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM files WHERE user_id = ? AND status = 'indexed'",
        (user_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def list_files(user_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM files WHERE user_id = ? ORDER BY created_at DESC",
        (user_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_checkpoint(job_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM job_checkpoints WHERE job_id = ?", (job_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def update_checkpoint(job_id: str, *, last_completed_page=None, total_pages=None, chunk_ids=None):
    conn = get_conn()
    cur = conn.cursor()

    fields = []
    values = []

    if last_completed_page is not None:
        fields.append("last_completed_page = ?")
        values.append(last_completed_page)
    if total_pages is not None:
        fields.append("total_pages = ?")
        values.append(total_pages)
    if chunk_ids is not None:
        fields.append("chunk_ids_json = ?")
        values.append(json.dumps(chunk_ids))

    fields.append("updated_at = CURRENT_TIMESTAMP")
    values.append(job_id)

    cur.execute(f"UPDATE job_checkpoints SET {', '.join(fields)} WHERE job_id = ?", values)
    conn.commit()
    conn.close()


def get_chunk_ids_for_job(job_id: str):
    checkpoint = get_checkpoint(job_id)
    if not checkpoint or not checkpoint.get("chunk_ids_json"):
        return []
    return json.loads(checkpoint["chunk_ids_json"])


def get_chunk_ids_for_file(file_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT chunk_ids_json
        FROM job_checkpoints
        WHERE file_id = ?
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (file_id,),
    )
    row = cur.fetchone()
    conn.close()

    if not row or not row["chunk_ids_json"]:
        return []
    return json.loads(row["chunk_ids_json"])


def delete_file_record(file_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM files WHERE id = ?", (file_id,))
    conn.commit()
    conn.close()


def delete_jobs_for_file(file_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM jobs WHERE file_id = ?", (file_id,))
    cur.execute("DELETE FROM job_checkpoints WHERE file_id = ?", (file_id,))
    conn.commit()
    conn.close()


def request_job_cancellation(job_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO job_cancellations (job_id, cancel_requested, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        """,
        (job_id, 1),
    )
    conn.commit()
    conn.close()


def is_cancellation_requested(job_id: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT cancel_requested FROM job_cancellations WHERE job_id = ?",
        (job_id,),
    )
    row = cur.fetchone()
    conn.close()
    return bool(row and row["cancel_requested"] == 1)


def clear_job_cancellation(job_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO job_cancellations (job_id, cancel_requested, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        """,
        (job_id, 0),
    )
    conn.commit()
    conn.close()