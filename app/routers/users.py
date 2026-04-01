from fastapi import APIRouter, Depends, File, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.services.user_ingestor import ingest_users

router = APIRouter()


@router.post("/users", summary="Upload users master CSV")
async def upload_users(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    contents = await file.read()
    result = await ingest_users(contents, db)
    return result