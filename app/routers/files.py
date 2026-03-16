from fastapi import APIRouter, UploadFile, File, Form, HTTPException, BackgroundTasks

from app.services.file_service import save_uploaded_file
from app.services.job_service import create_index_job
from app.services.indexing_service import index_pdf_file

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

    if file_record["file_type"] == ".pdf":
        background_tasks.add_task(
            index_pdf_file,
            job_id,
            file_record["id"],
            user_id,
        )
        message = "File uploaded successfully. PDF indexing started in background."
    else:
        message = "File uploaded successfully. Indexing not implemented yet for this file type."

    return {
        "message": message,
        "file": file_record,
        "job_id": job_id,
    }