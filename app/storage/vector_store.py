from haystack_integrations.document_stores.chroma import ChromaDocumentStore
from app.config import CHROMA_DIR
from app import state


def get_user_document_store(user_id: str):
    if user_id not in state.document_stores:
        state.document_stores[user_id] = ChromaDocumentStore(
            persist_path=str(CHROMA_DIR / user_id),
            collection_name=f"user_{user_id}_documents",
        )
    return state.document_stores[user_id]


def reset_user_document_store(user_id: str):
    state.document_stores.pop(user_id, None)