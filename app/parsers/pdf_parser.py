from pypdf import PdfReader


def get_pdf_page_count(file_path: str) -> int:
    reader = PdfReader(file_path)
    return len(reader.pages)


def extract_pdf_pages(file_path: str, start_page: int, end_page: int):
    reader = PdfReader(file_path)
    documents = []

    for page_index in range(start_page, min(end_page, len(reader.pages))):
        page = reader.pages[page_index]
        text = page.extract_text() or ""

        if text.strip():
            documents.append(
                {
                    "page_number": page_index + 1,
                    "text": text,
                }
            )

    return documents