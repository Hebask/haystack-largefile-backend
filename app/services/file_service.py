from pathlib import Path
import shutil

from app.config import UPLOAD_DIR
from app.db import get_conn
from app.utils.ids import new_id

ALLOWED_EXTENSIONS = {".pdf", ".docx", ".txt", ".xlsx"}

def save_uploaded_file(user_id: str, upload_file):
    ext = Path(upload_file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise ValueError(f"Unsupported file type: {ext}")

    file_id = new_id()
    user_dir = UPLOAD_DIR / user_id
    user_dir.mkdir(parents=True, exist_ok=True)

    stored_name = f"{file_id}{ext}"
    stored_path = user_dir / stored_name

    with stored_path.open("wb") as buffer:
        shutil.copyfileobj(upload_file.file, buffer)

    file_size = stored_path.stat().st_size

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO files (id, user_id, original_name, stored_path, file_type, file_size, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (file_id, user_id, upload_file.filename, str(stored_path), ext, file_size, "uploaded"),
    )
    conn.commit()
    conn.close()

    return {
        "id": file_id,
        "user_id": user_id,
        "original_name": upload_file.filename,
        "stored_path": str(stored_path),
        "file_type": ext,
        "file_size": file_size,
        "status": "uploaded",
    }