from pathlib import Path
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, BackgroundTasks, Query
from typing import List

from app.services.file_service import save_uploaded_file
from app.services.job_service import (
    create_index_job,
    get_file_record,
    get_chunk_ids_for_file,
    delete_file_record,
    delete_jobs_for_file,
)
from app.services.indexing_service import start_index_job
from app.services.query_service import reset_query_pipeline
from app.storage.vector_store import get_user_document_store

router = APIRouter(prefix="/files", tags=["files"])


@router.post("/upload")
def upload_file(
    background_tasks: BackgroundTasks,
    user_id: str = Form(...),
    file: UploadFile = File(...),
):
    try:
        file_record = save_uploaded_file(user_id, file)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    job_id = create_index_job(file_record["id"], user_id)

    background_tasks.add_task(
        start_index_job,
        job_id,
        file_record["id"],
        user_id,
    )

    return {
        "message": "File uploaded successfully. Indexing started in background if supported.",
        "file": file_record,
        "job_id": job_id,
    }


@router.post("/upload-batch")
def upload_batch(
    background_tasks: BackgroundTasks,
    user_id: str = Form(...),
    files: List[UploadFile] = File(...),
):
    uploaded = []
    failed = []

    for file in files:
        try:
            file_record = save_uploaded_file(user_id, file)
            job_id = create_index_job(file_record["id"], user_id)

            background_tasks.add_task(
                start_index_job,
                job_id,
                file_record["id"],
                user_id,
            )

            uploaded.append(
                {
                    "file": file_record,
                    "job_id": job_id,
                }
            )
        except Exception as e:
            failed.append(
                {
                    "filename": file.filename,
                    "error": str(e),
                }
            )

    return {
        "message": "Batch upload processed",
        "uploaded_count": len(uploaded),
        "failed_count": len(failed),
        "uploaded": uploaded,
        "failed": failed,
    }


@router.delete("/{file_id}")
def delete_file(file_id: str, user_id: str = Query(...)):
    file_record = get_file_record(file_id)
    if not file_record or file_record["user_id"] != user_id:
        raise HTTPException(status_code=404, detail="File not found")

    chunk_ids = get_chunk_ids_for_file(file_id)
    if chunk_ids:
        document_store = get_user_document_store(user_id)
        document_store.delete_documents(chunk_ids)

    stored_path = Path(file_record["stored_path"])
    if stored_path.exists():
        stored_path.unlink()

    delete_file_record(file_id)
    delete_jobs_for_file(file_id)
    reset_query_pipeline(user_id)

    return {
        "message": "File and indexed chunks deleted",
        "file_id": file_id,
        "deleted_chunk_count": len(chunk_ids),
    }