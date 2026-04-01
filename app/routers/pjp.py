from fastapi import APIRouter, Depends, File, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.services.pjp_ingestor import ingest_pjp

router = APIRouter()


@router.post("/pjp", summary="Upload store-user mapping CSV")
async def upload_pjp(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    contents = await file.read()
    result = await ingest_pjp(contents, db)
    return result