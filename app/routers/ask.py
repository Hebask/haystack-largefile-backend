from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from haystack import Pipeline
from haystack.components.embedders import SentenceTransformersTextEmbedder
from haystack.components.builders import ChatPromptBuilder
from haystack.components.generators.chat import OpenAIChatGenerator
from haystack.dataclasses import ChatMessage

from haystack_integrations.components.retrievers.chroma import ChromaEmbeddingRetriever

from app.config import EMBED_MODEL, CHAT_MODEL, TOP_K
from app.storage.vector_store import get_user_document_store
from app.services.job_service import list_indexed_files

router = APIRouter(prefix="/ask", tags=["ask"])


class AskRequest(BaseModel):
    user_id: str
    question: str


RAG_PROMPT = """
Answer the user's question using only the retrieved chunks below.
If the answer is not present, say:
I could not find that in the indexed files.

Retrieved chunks:
{% for doc in documents %}
---
File: {{ doc.meta.get("file_path", "unknown") }}
Page: {{ doc.meta.get("page_number", "n/a") }}
Content:
{{ doc.content }}
{% endfor %}

Question: {{ question }}
"""


def build_query_pipeline(user_id: str):
    query = Pipeline()

    text_embedder = SentenceTransformersTextEmbedder(model=EMBED_MODEL)
    text_embedder.warm_up()

    query.add_component("text_embedder", text_embedder)
    query.add_component(
        "retriever",
        ChromaEmbeddingRetriever(
            document_store=get_user_document_store(user_id),
            top_k=TOP_K,
        ),
    )
    query.add_component(
        "prompt_builder",
        ChatPromptBuilder(
            template=[ChatMessage.from_user(RAG_PROMPT)],
            required_variables=["question", "documents"],
        ),
    )
    query.add_component(
        "llm",
        OpenAIChatGenerator(model=CHAT_MODEL),
    )

    query.connect("text_embedder.embedding", "retriever.query_embedding")
    query.connect("retriever.documents", "prompt_builder.documents")
    query.connect("prompt_builder.prompt", "llm.messages")

    return query


@router.post("")
def ask_question(payload: AskRequest):
    indexed_files = list_indexed_files(payload.user_id)
    if not indexed_files:
        raise HTTPException(
            status_code=400,
            detail="No indexed files are available yet for this user.",
        )

    pipeline = build_query_pipeline(payload.user_id)

    result = pipeline.run(
        {
            "text_embedder": {"text": payload.question},
            "prompt_builder": {"question": payload.question},
        },
        include_outputs_from={"retriever", "llm"},
    )

    docs = result["retriever"]["documents"]
    sources = []
    seen = set()

    for doc in docs:
        item = {
            "file": doc.meta.get("file_path", "unknown"),
            "page": doc.meta.get("page_number", "n/a"),
            "preview": (doc.content or "")[:250].replace("\n", " "),
        }
        key = (item["file"], item["page"], item["preview"])
        if key not in seen:
            seen.add(key)
            sources.append(item)

    return {
        "answer": result["llm"]["replies"][0].text,
        "sources": sources,
    }