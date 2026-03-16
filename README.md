# Haystack Large File Backend

A job-based large-file question answering backend built with **FastAPI**, **Haystack**, **Chroma**, and **OpenAI**.

This project is designed for **large-file ingestion** and retrieval, with support for:

- background indexing jobs
- job progress tracking
- retry and cancellation
- persistent vector storage
- multi-file upload
- per-file filtering during question answering
- file deletion with vector cleanup

It is designed as a better architecture for large files than synchronous upload-and-index workflows.

---

## Features

- **Job-based architecture**
  - upload returns quickly
  - indexing runs in background
  - job progress can be checked anytime

- **Supported file types**
  - PDF
  - DOCX
  - TXT
  - XLSX

- **Persistent vector storage**
  - Chroma document store
  - embeddings are reused across restarts

- **Large-file friendly design**
  - PDF indexed in page batches
  - DOCX indexed by paragraphs
  - TXT indexed by text blocks
  - XLSX indexed by row blocks

- **Operational controls**
  - retry failed/cancelled jobs
  - cancel running jobs
  - delete files and indexed chunks

- **Question answering**
  - ask across all indexed files
  - optionally restrict answers to selected file IDs
  - returns source citations

---

## Architecture Overview

### Upload flow
1. User uploads a file
2. File is saved to disk
3. A job record is created
4. Background indexing starts

### Indexing flow
1. File is parsed into logical units
2. Text is chunked using Haystack preprocessing
3. Chunks are embedded
4. Embeddings are stored in Chroma
5. Job checkpoint and progress are updated

### Retrieval flow
1. User sends a question
2. System checks indexed files
3. Query embedding is created
4. Relevant chunks are retrieved from Chroma
5. OpenAI generates the answer using retrieved chunks
6. Sources are returned

---

## Project Structure

```text
haystack-largefile-backend/
├─ app/
│  ├─ __init__.py
│  ├─ main.py
│  ├─ config.py
│  ├─ db.py
│  ├─ state.py
│  ├─ routers/
│  │  ├─ files.py
│  │  ├─ jobs.py
│  │  └─ ask.py
│  ├─ services/
│  │  ├─ file_service.py
│  │  ├─ job_service.py
│  │  ├─ indexing_service.py
│  │  └─ query_service.py
│  ├─ storage/
│  │  └─ vector_store.py
│  ├─ parsers/
│  │  ├─ pdf_parser.py
│  │  ├─ docx_parser.py
│  │  ├─ txt_parser.py
│  │  └─ xlsx_parser.py
│  └─ utils/
│     └─ ids.py
├─ data/
│  ├─ uploads/
│  ├─ chroma/
│  └─ app.db
├─ requirements.txt
├─ .env
├─ .env.example
├─ .gitignore
├─ Dockerfile
└─ README.md