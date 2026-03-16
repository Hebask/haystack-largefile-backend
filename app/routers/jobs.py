from fastapi import APIRouter, HTTPException, Query, BackgroundTasks

from app.services.job_service import (
    list_jobs,
    get_job,
    request_job_cancellation,
    create_index_job,
    get_file_record,
)
from app.services.indexing_service import start_index_job

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("")
def jobs(user_id: str = Query(...)):
    return {"jobs": list_jobs(user_id)}


@router.get("/{job_id}")
def job_status(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.post("/{job_id}/cancel")
def cancel_job(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job["status"] in {"completed", "failed", "cancelled"}:
        return {"message": f"Job already in terminal state: {job['status']}", "job_id": job_id}

    request_job_cancellation(job_id)
    return {"message": "Cancellation requested", "job_id": job_id}


@router.post("/{job_id}/retry")
def retry_job(job_id: str, background_tasks: BackgroundTasks):
    old_job = get_job(job_id)
    if not old_job:
        raise HTTPException(status_code=404, detail="Job not found")

    file_record = get_file_record(old_job["file_id"])
    if not file_record:
        raise HTTPException(status_code=404, detail="Associated file not found")

    new_job_id = create_index_job(old_job["file_id"], old_job["user_id"])

    background_tasks.add_task(
        start_index_job,
        new_job_id,
        old_job["file_id"],
        old_job["user_id"],
    )

    return {
        "message": "Retry job created",
        "old_job_id": job_id,
        "new_job_id": new_job_id,
    }