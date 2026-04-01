from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import init_db
from app.routers import pjp, stores, users


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(
    title="Infilect Data Ingestion API",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(stores.router, prefix="/api/upload", tags=["Stores"])
app.include_router(users.router, prefix="/api/upload", tags=["Users"])
app.include_router(pjp.router, prefix="/api/upload", tags=["PJP"])


@app.get("/health")
async def health():
    return {"status": "ok"}