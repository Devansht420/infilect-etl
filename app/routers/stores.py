from fastapi import APIRouter, Depends, File, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.services.store_ingestor import ingest_stores

router = APIRouter()


@router.post("/stores", summary="Upload stores master CSV")
async def upload_stores(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    contents = await file.read()
    result = await ingest_stores(contents, db)
    return result