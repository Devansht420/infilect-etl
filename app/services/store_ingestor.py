import asyncio
import io
from typing import Any

import pandas as pd
from pydantic import ValidationError
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.store import Store
from app.schemas.store_schema import StoreRowSchema
from app.services.lookup_service import LookupCache, get_or_create_lookup

LOOKUP_FIELDS = ["store_brand", "store_type", "city", "state", "country", "region"]


async def ingest_stores(contents: bytes, session: AsyncSession) -> dict:
    errors: list[dict] = []
    total = 0
    inserted = 0

    cache = LookupCache()

    existing_ids_result = await session.execute(select(Store.store_id))
    existing_store_ids: set[str] = {row[0] for row in existing_ids_result}
    seen_in_file: set[str] = set()

    df_iter = pd.read_csv(
        io.BytesIO(contents),
        chunksize=settings.CHUNK_SIZE,
        dtype=str,
        keep_default_na=False,
    )

    for chunk in df_iter:
        valid_rows: list[dict[str, Any]] = []

        for _, row in chunk.iterrows():
            abs_row = row.name + 2
            total += 1

            row_dict = row.to_dict()

            try:
                validated = StoreRowSchema(**row_dict)
            except ValidationError as e:
                for err in e.errors():
                    field = err["loc"][0] if err["loc"] else "unknown"
                    errors.append({
                        "row": abs_row,
                        "column": field,
                        "reason": err["msg"],
                    })
                continue

            sid = validated.store_id
            if sid in existing_store_ids or sid in seen_in_file:
                errors.append({
                    "row": abs_row,
                    "column": "store_id",
                    "reason": f"duplicate store_id '{sid}'",
                })
                continue
            seen_in_file.add(sid)

            try:
                lookup_ids = {}
                for field in LOOKUP_FIELDS:
                    raw_val = getattr(validated, field)
                    lookup_ids[f"{field}_id"] = await get_or_create_lookup(
                        session, cache, field, raw_val
                    )
            except Exception as exc:
                errors.append({
                    "row": abs_row,
                    "column": "lookup",
                    "reason": str(exc),
                })
                continue

            valid_rows.append({
                "store_id": validated.store_id,
                "store_external_id": validated.store_external_id or "",
                "name": validated.name,
                "title": validated.title,
                "latitude": validated.latitude,
                "longitude": validated.longitude,
                **lookup_ids,
            })

        if valid_rows:
            await session.execute(insert(Store), valid_rows)
            await session.commit()
            existing_store_ids.update(r["store_id"] for r in valid_rows)
            inserted += len(valid_rows)

    return {
        "total_rows": total,
        "inserted": inserted,
        "failed": total - inserted,
        "errors": errors,
    }