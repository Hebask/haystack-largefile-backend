from fastapi import APIRouter, HTTPException, Query
from app.services.job_service import list_jobs, get_job

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