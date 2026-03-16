from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.db import init_db
from app.routers.files import router as files_router
from app.routers.jobs import router as jobs_router
from app.routers.ask import router as ask_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Haystack Large File Backend",
    lifespan=lifespan,
)

app.include_router(files_router)
app.include_router(jobs_router)
app.include_router(ask_router)


@app.get("/")
def root():
    return {"message": "Haystack Large File Backend is running"}