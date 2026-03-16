from typing import Optional, List
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.job_service import list_indexed_files
from app.services.query_service import get_query_pipeline

router = APIRouter(prefix="/ask", tags=["ask"])


class AskRequest(BaseModel):
    user_id: str
    question: str
    file_ids: Optional[List[str]] = None


@router.post("")
def ask_question(payload: AskRequest):
    indexed_files = list_indexed_files(payload.user_id)
    if not indexed_files:
        raise HTTPException(
            status_code=400,
            detail="No indexed files are available yet for this user.",
        )

    allowed_ids = None
    if payload.file_ids:
        allowed_ids = set(payload.file_ids)

    pipeline = get_query_pipeline(payload.user_id)

    result = pipeline.run(
        {
            "text_embedder": {"text": payload.question},
            "prompt_builder": {"question": payload.question},
        },
        include_outputs_from={"retriever", "llm"},
    )

    docs = result["retriever"]["documents"]

    if allowed_ids is not None:
        docs = [d for d in docs if d.meta.get("file_id") in allowed_ids]

    if not docs:
        return {
            "indexed_files_count": len(indexed_files),
            "answer": "I could not find that in the selected indexed files.",
            "sources": [],
        }

    sources = []
    seen = set()

    for doc in docs:
        item = {
            "file_id": doc.meta.get("file_id"),
            "file": doc.meta.get("file_path", "unknown"),
            "page": doc.meta.get("page_number", "n/a"),
            "preview": (doc.content or "")[:250].replace("\n", " "),
        }
        key = (item["file_id"], item["file"], item["page"], item["preview"])
        if key not in seen:
            seen.add(key)
            sources.append(item)

    return {
        "indexed_files_count": len(indexed_files),
        "answer": result["llm"]["replies"][0].text,
        "sources": sources,
    }