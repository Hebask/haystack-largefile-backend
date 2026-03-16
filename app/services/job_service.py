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

    cur.execute(
        f"UPDATE jobs SET {', '.join(fields)} WHERE id = ?",
        values,
    )
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