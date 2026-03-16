from haystack import Document
from haystack.components.preprocessors import DocumentPreprocessor
from haystack.components.embedders import SentenceTransformersDocumentEmbedder

from app.config import EMBED_MODEL
from app.parsers.pdf_parser import get_pdf_page_count, extract_pdf_pages
from app.parsers.docx_parser import extract_docx_paragraphs
from app.parsers.txt_parser import extract_txt_blocks
from app.parsers.xlsx_parser import extract_xlsx_blocks
from app.storage.vector_store import get_user_document_store
from app.services.job_service import (
    update_job,
    set_file_status,
    get_file_record,
    get_checkpoint,
    update_checkpoint,
    get_chunk_ids_for_job,
    is_cancellation_requested,
    clear_job_cancellation,
)

BATCH_PAGES = 25


def make_chunk_id(user_id: str, file_id: str, unit_number: int, chunk_index: int, prefix: str) -> str:
    return f"{user_id}:{file_id}:{prefix}{unit_number}:c{chunk_index}"


def _build_embedder_and_preprocessor():
    preprocessor = DocumentPreprocessor(
        split_by="word",
        split_length=120,
        split_overlap=30,
    )
    embedder = SentenceTransformersDocumentEmbedder(model=EMBED_MODEL)
    embedder.warm_up()
    return preprocessor, embedder


def _clean_and_embed(documents, user_id, file_id, file_name, unit_key, unit_prefix, tracked_chunk_ids, document_store, preprocessor, embedder):
    split_docs = preprocessor.run(documents=documents)["documents"]

    fixed_docs = []
    unit_chunk_counters = {}

    for doc in split_docs:
        unit_number = int(doc.meta.get(unit_key, 0))
        unit_chunk_counters.setdefault(unit_number, 0)
        unit_chunk_counters[unit_number] += 1
        chunk_index = unit_chunk_counters[unit_number]

        clean_meta = {}
        for key, value in (doc.meta or {}).items():
            if isinstance(value, (str, int, float, bool)):
                clean_meta[key] = value

        chunk_id = make_chunk_id(
            user_id=user_id,
            file_id=file_id,
            unit_number=unit_number,
            chunk_index=chunk_index,
            prefix=unit_prefix,
        )

        clean_meta["chunk_id"] = chunk_id
        clean_meta["file_id"] = file_id
        clean_meta["file_path"] = file_name
        clean_meta["user_id"] = user_id

        fixed_docs.append(
            Document(
                id=chunk_id,
                content=doc.content,
                meta=clean_meta,
            )
        )

        if chunk_id not in tracked_chunk_ids:
            tracked_chunk_ids.append(chunk_id)

    embedded_docs = embedder.run(documents=fixed_docs)["documents"]
    document_store.write_documents(embedded_docs)
    return tracked_chunk_ids


def index_pdf_file(job_id: str, file_id: str, user_id: str):
    file_record = get_file_record(file_id)
    if not file_record:
        update_job(job_id, status="failed", error="File record not found")
        return

    file_path = file_record["stored_path"]

    try:
        clear_job_cancellation(job_id)

        checkpoint = get_checkpoint(job_id)
        last_completed_page = checkpoint["last_completed_page"] if checkpoint else 0
        tracked_chunk_ids = get_chunk_ids_for_job(job_id)

        update_job(job_id, status="running", progress=0, message="Starting PDF indexing")
        set_file_status(file_id, "indexing")

        total_pages = get_pdf_page_count(file_path)
        update_checkpoint(job_id, total_pages=total_pages)

        document_store = get_user_document_store(user_id)
        preprocessor, embedder = _build_embedder_and_preprocessor()

        for start_page in range(last_completed_page, total_pages, BATCH_PAGES):
            if is_cancellation_requested(job_id):
                set_file_status(file_id, "uploaded")
                update_job(job_id, status="cancelled", message="Cancellation requested by user")
                return

            end_page = min(start_page + BATCH_PAGES, total_pages)
            page_docs = extract_pdf_pages(file_path, start_page, end_page)

            haystack_docs = []
            for page_doc in page_docs:
                haystack_docs.append(
                    Document(
                        content=page_doc["text"],
                        meta={"page_number": int(page_doc["page_number"])},
                    )
                )

            if haystack_docs:
                tracked_chunk_ids = _clean_and_embed(
                    documents=haystack_docs,
                    user_id=user_id,
                    file_id=file_id,
                    file_name=file_record["original_name"],
                    unit_key="page_number",
                    unit_prefix="p",
                    tracked_chunk_ids=tracked_chunk_ids,
                    document_store=document_store,
                    preprocessor=preprocessor,
                    embedder=embedder,
                )

            processed_pages = end_page
            progress = int((processed_pages / total_pages) * 100)

            update_checkpoint(
                job_id,
                last_completed_page=processed_pages,
                total_pages=total_pages,
                chunk_ids=tracked_chunk_ids,
            )

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


def index_docx_file(job_id: str, file_id: str, user_id: str):
    file_record = get_file_record(file_id)
    if not file_record:
        update_job(job_id, status="failed", error="File record not found")
        return

    try:
        clear_job_cancellation(job_id)
        tracked_chunk_ids = get_chunk_ids_for_job(job_id)

        update_job(job_id, status="running", progress=0, message="Starting DOCX indexing")
        set_file_status(file_id, "indexing")

        paragraphs = extract_docx_paragraphs(file_record["stored_path"])
        total_units = max(len(paragraphs), 1)
        update_checkpoint(job_id, total_pages=total_units)

        document_store = get_user_document_store(user_id)
        preprocessor, embedder = _build_embedder_and_preprocessor()

        haystack_docs = [
            Document(content=item["text"], meta={"paragraph_number": int(item["paragraph_number"])})
            for item in paragraphs
        ]

        if haystack_docs:
            tracked_chunk_ids = _clean_and_embed(
                documents=haystack_docs,
                user_id=user_id,
                file_id=file_id,
                file_name=file_record["original_name"],
                unit_key="paragraph_number",
                unit_prefix="para",
                tracked_chunk_ids=tracked_chunk_ids,
                document_store=document_store,
                preprocessor=preprocessor,
                embedder=embedder,
            )

        update_checkpoint(
            job_id,
            last_completed_page=total_units,
            total_pages=total_units,
            chunk_ids=tracked_chunk_ids,
        )

        set_file_status(file_id, "indexed")
        update_job(job_id, status="completed", progress=100, message="DOCX indexing completed")

    except Exception as e:
        set_file_status(file_id, "failed")
        update_job(job_id, status="failed", error=str(e), message="DOCX indexing failed")


def index_txt_file(job_id: str, file_id: str, user_id: str):
    file_record = get_file_record(file_id)
    if not file_record:
        update_job(job_id, status="failed", error="File record not found")
        return

    try:
        clear_job_cancellation(job_id)
        tracked_chunk_ids = get_chunk_ids_for_job(job_id)

        update_job(job_id, status="running", progress=0, message="Starting TXT indexing")
        set_file_status(file_id, "indexing")

        blocks = extract_txt_blocks(file_record["stored_path"])
        total_units = max(len(blocks), 1)
        update_checkpoint(job_id, total_pages=total_units)

        document_store = get_user_document_store(user_id)
        preprocessor, embedder = _build_embedder_and_preprocessor()

        haystack_docs = [
            Document(content=item["text"], meta={"block_number": int(item["block_number"])})
            for item in blocks
        ]

        if haystack_docs:
            tracked_chunk_ids = _clean_and_embed(
                documents=haystack_docs,
                user_id=user_id,
                file_id=file_id,
                file_name=file_record["original_name"],
                unit_key="block_number",
                unit_prefix="blk",
                tracked_chunk_ids=tracked_chunk_ids,
                document_store=document_store,
                preprocessor=preprocessor,
                embedder=embedder,
            )

        update_checkpoint(
            job_id,
            last_completed_page=total_units,
            total_pages=total_units,
            chunk_ids=tracked_chunk_ids,
        )

        set_file_status(file_id, "indexed")
        update_job(job_id, status="completed", progress=100, message="TXT indexing completed")

    except Exception as e:
        set_file_status(file_id, "failed")
        update_job(job_id, status="failed", error=str(e), message="TXT indexing failed")


def index_xlsx_file(job_id: str, file_id: str, user_id: str):
    file_record = get_file_record(file_id)
    if not file_record:
        update_job(job_id, status="failed", error="File record not found")
        return

    try:
        clear_job_cancellation(job_id)
        tracked_chunk_ids = get_chunk_ids_for_job(job_id)

        update_job(job_id, status="running", progress=0, message="Starting XLSX indexing")
        set_file_status(file_id, "indexing")

        blocks = extract_xlsx_blocks(file_record["stored_path"])
        total_units = max(len(blocks), 1)
        update_checkpoint(job_id, total_pages=total_units)

        document_store = get_user_document_store(user_id)
        preprocessor, embedder = _build_embedder_and_preprocessor()

        haystack_docs = [
            Document(
                content=item["text"],
                meta={
                    "block_number": int(item["block_number"]),
                    "sheet_name": item["sheet_name"],
                },
            )
            for item in blocks
        ]

        if haystack_docs:
            tracked_chunk_ids = _clean_and_embed(
                documents=haystack_docs,
                user_id=user_id,
                file_id=file_id,
                file_name=file_record["original_name"],
                unit_key="block_number",
                unit_prefix="xls",
                tracked_chunk_ids=tracked_chunk_ids,
                document_store=document_store,
                preprocessor=preprocessor,
                embedder=embedder,
            )

        update_checkpoint(
            job_id,
            last_completed_page=total_units,
            total_pages=total_units,
            chunk_ids=tracked_chunk_ids,
        )

        set_file_status(file_id, "indexed")
        update_job(job_id, status="completed", progress=100, message="XLSX indexing completed")

    except Exception as e:
        set_file_status(file_id, "failed")
        update_job(job_id, status="failed", error=str(e), message="XLSX indexing failed")


def start_index_job(job_id: str, file_id: str, user_id: str):
    file_record = get_file_record(file_id)
    if not file_record:
        update_job(job_id, status="failed", error="File record not found")
        return

    ext = file_record["file_type"].lower()

    if ext == ".pdf":
        return index_pdf_file(job_id, file_id, user_id)
    if ext == ".docx":
        return index_docx_file(job_id, file_id, user_id)
    if ext == ".txt":
        return index_txt_file(job_id, file_id, user_id)
    if ext == ".xlsx":
        return index_xlsx_file(job_id, file_id, user_id)

    update_job(job_id, status="failed", error=f"Unsupported indexing type: {ext}")
    set_file_status(file_id, "uploaded")