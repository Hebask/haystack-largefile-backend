from haystack_integrations.document_stores.chroma import ChromaDocumentStore
from app.config import CHROMA_DIR

_document_stores = {}


def get_user_document_store(user_id: str):
    if user_id not in _document_stores:
        _document_stores[user_id] = ChromaDocumentStore(
            persist_path=str(CHROMA_DIR / user_id),
            collection_name=f"user_{user_id}_documents",
        )
    return _document_stores[user_id]


def reset_user_document_store(user_id: str):
    _document_stores.pop(user_id, None)