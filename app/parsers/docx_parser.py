from docx import Document as DocxDocument


def extract_docx_paragraphs(file_path: str):
    doc = DocxDocument(file_path)
    paragraphs = []

    for idx, paragraph in enumerate(doc.paragraphs, start=1):
        text = (paragraph.text or "").strip()
        if text:
            paragraphs.append(
                {
                    "paragraph_number": idx,
                    "text": text,
                }
            )

    return paragraphs