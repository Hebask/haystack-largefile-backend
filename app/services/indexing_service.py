from haystack import Document
from haystack.components.preprocessors import DocumentPreprocessor
from haystack.components.embedders import SentenceTransformersDocumentEmbedder

from app.config import EMBED_MODEL
from app.parsers.pdf_parser import get_pdf_page_count, extract_pdf_pages
from app.storage.vector_store import get_user_document_store
from app.services.job_service import (
    update_job,
    set_file_status,
    get_file_record,
)


BATCH_PAGES = 25


def make_chunk_id(user_id: str, file_id: str, page_number: int, chunk_index: int) -> str:
    return f"{user_id}:{file_id}:p{page_number}:c{chunk_index}"


def index_pdf_file(job_id: str, file_id: str, user_id: str):
    file_record = get_file_record(file_id)
    if not file_record:
        update_job(job_id, status="failed", error="File record not found")
        return

    file_path = file_record["stored_path"]

    try:
        update_job(job_id, status="running", progress=0, message="Starting PDF indexing")
        set_file_status(file_id, "indexing")

        total_pages = get_pdf_page_count(file_path)
        document_store = get_user_document_store(user_id)

        preprocessor = DocumentPreprocessor(
            split_by="word",
            split_length=120,
            split_overlap=30,
        )

        embedder = SentenceTransformersDocumentEmbedder(model=EMBED_MODEL)
        embedder.warm_up()

        processed_pages = 0

        for start_page in range(0, total_pages, BATCH_PAGES):
            end_page = min(start_page + BATCH_PAGES, total_pages)
            page_docs = extract_pdf_pages(file_path, start_page, end_page)

            haystack_docs = []
            for page_doc in page_docs:
                page_number = page_doc["page_number"]
                text = page_doc["text"]

                haystack_docs.append(
                    Document(
                        content=text,
                        meta={
                            "file_id": file_id,
                            "file_path": file_record["original_name"],
                            "page_number": int(page_number),
                            "user_id": user_id,
                        },
                    )
                )

            if haystack_docs:
                split_docs = preprocessor.run(documents=haystack_docs)["documents"]

                fixed_docs = []
                page_chunk_counters = {}

                for doc in split_docs:
                    page_number = int(doc.meta.get("page_number", 0))
                    page_chunk_counters.setdefault(page_number, 0)
                    page_chunk_counters[page_number] += 1
                    chunk_index = page_chunk_counters[page_number]

                    clean_meta = {}
                    for key, value in (doc.meta or {}).items():
                        if isinstance(value, (str, int, float, bool)):
                            clean_meta[key] = value

                    clean_meta["chunk_id"] = make_chunk_id(
                        user_id=user_id,
                        file_id=file_id,
                        page_number=page_number,
                        chunk_index=chunk_index,
                    )

                    fixed_docs.append(
                        Document(
                            id=clean_meta["chunk_id"],
                            content=doc.content,
                            meta=clean_meta,
                        )
                    )

                embedded_docs = embedder.run(documents=fixed_docs)["documents"]
                document_store.write_documents(embedded_docs)

            processed_pages = end_page
            progress = int((processed_pages / total_pages) * 100)
            update_job(
                job_id,
                status="running",
                progress=progress,
                message=f"Indexed pages {start_page + 1}-{end_page} of {total_pages}",
            )

        set_file_status(file_id, "indexed")
        update_job(job_id, status="completed", progress=100, message="Indexing completed")

    except Exception as e:
        set_file_status(file_id, "failed")
        update_job(job_id, status="failed", error=str(e), message="Indexing failed")