import io

import pandas as pd
from pydantic import ValidationError
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.pjp import PermanentJourneyPlan
from app.models.store import Store
from app.models.user import User
from app.schemas.pjp_schema import PJPRowSchema


async def ingest_pjp(contents: bytes, session: AsyncSession) -> dict:
    errors: list[dict] = []
    total = 0
    inserted = 0

    user_result = await session.execute(select(User.id, User.username))
    username_to_id: dict[str, int] = {row.username: row.id for row in user_result}

    store_result = await session.execute(select(Store.id, Store.store_id))
    storeid_to_id: dict[str, int] = {row.store_id: row.id for row in store_result}

    existing_result = await session.execute(
        select(PermanentJourneyPlan.user_id, PermanentJourneyPlan.store_id, PermanentJourneyPlan.date)
    )
    existing_triplets: set[tuple] = {(r.user_id, r.store_id, r.date) for r in existing_result}
    seen_in_file: set[tuple] = set()

    df_iter = pd.read_csv(
        io.BytesIO(contents),
        chunksize=settings.CHUNK_SIZE,
        dtype=str,
        keep_default_na=False,
    )

    for chunk in df_iter:
        valid_rows = []

        for _, row in chunk.iterrows():
            abs_row = row.name + 2
            total += 1
            row_dict = row.to_dict()

            try:
                validated = PJPRowSchema(**row_dict)
            except ValidationError as e:
                for err in e.errors():
                    field = err["loc"][0] if err["loc"] else "unknown"
                    errors.append({"row": abs_row, "column": field, "reason": err["msg"]})
                continue

            user_id = username_to_id.get(validated.username)
            if user_id is None:
                errors.append({
                    "row": abs_row,
                    "column": "username",
                    "reason": f"user '{validated.username}' does not exist",
                })
                continue

            store_db_id = storeid_to_id.get(validated.store_id)
            if store_db_id is None:
                errors.append({
                    "row": abs_row,
                    "column": "store_id",
                    "reason": f"store '{validated.store_id}' does not exist",
                })
                continue

            parsed_date = validated.get_date()
            triplet = (user_id, store_db_id, parsed_date)
            if triplet in existing_triplets or triplet in seen_in_file:
                errors.append({
                    "row": abs_row,
                    "column": "user_id/store_id/date",
                    "reason": f"duplicate mapping: user='{validated.username}' store='{validated.store_id}' date='{parsed_date}'",
                })
                continue
            seen_in_file.add(triplet)

            valid_rows.append({
                "user_id": user_id,
                "store_id": store_db_id,
                "date": parsed_date,
                "is_active": validated.is_active,
            })

        if valid_rows:
            await session.execute(insert(PermanentJourneyPlan), valid_rows)
            await session.commit()
            existing_triplets.update((r["user_id"], r["store_id"], r["date"]) for r in valid_rows)
            inserted += len(valid_rows)

    return {
        "total_rows": total,
        "inserted": inserted,
        "failed": total - inserted,
        "errors": errors,
    }