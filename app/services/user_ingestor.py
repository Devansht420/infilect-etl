import io
from typing import Any

import pandas as pd
from pydantic import ValidationError
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.user import User
from app.schemas.user_schema import UserRowSchema


async def ingest_users(contents: bytes, session: AsyncSession) -> dict:
    errors: list[dict] = []
    total = 0
    inserted = 0

    existing_result = await session.execute(select(User.username))
    existing_usernames: set[str] = {row[0] for row in existing_result}
    seen_in_file: set[str] = set()

    all_valid: list[dict[str, Any]] = []
    supervisor_map: dict[str, tuple[str, int]] = {}

    df_iter = pd.read_csv(
        io.BytesIO(contents),
        chunksize=settings.CHUNK_SIZE,
        dtype=str,
        keep_default_na=False,
    )

    for chunk in df_iter:
        for _, row in chunk.iterrows():
            abs_row = row.name + 2
            total += 1
            row_dict = row.to_dict()

            try:
                validated = UserRowSchema(**row_dict)
            except ValidationError as e:
                for err in e.errors():
                    field = err["loc"][0] if err["loc"] else "unknown"
                    errors.append({"row": abs_row, "column": field, "reason": err["msg"]})
                continue

            uname = validated.username
            if uname in existing_usernames or uname in seen_in_file:
                errors.append({
                    "row": abs_row,
                    "column": "username",
                    "reason": f"duplicate username '{uname}'",
                })
                continue
            seen_in_file.add(uname)

            if validated.supervisor_username:
                sup = validated.supervisor_username
                supervisor_map[uname] = (sup, abs_row)

            all_valid.append({
                "username": validated.username,
                "first_name": validated.first_name,
                "last_name": validated.last_name,
                "email": validated.email,
                "user_type": validated.user_type,
                "phone_number": validated.phone_number,
                "is_active": validated.is_active,
                "supervisor_id": None,
            })

    if all_valid:
        for i in range(0, len(all_valid), settings.CHUNK_SIZE):
            chunk_data = all_valid[i : i + settings.CHUNK_SIZE]
            await session.execute(insert(User), chunk_data)
        await session.commit()
        inserted = len(all_valid)

    supervisor_errors = []
    if supervisor_map:
        usernames_needing_supervisor = {sup for sup, _ in supervisor_map.values()}
        result = await session.execute(
            select(User.id, User.username).where(
                User.username.in_(usernames_needing_supervisor)
            )
        )
        username_to_id: dict[str, int] = {row.username: row.id for row in result}

        for username, (sup_username, row_num) in supervisor_map.items():
            if sup_username not in username_to_id:
                supervisor_errors.append({
                    "row": row_num,
                    "column": "supervisor_username",
                    "reason": f"supervisor '{sup_username}' not found for user '{username}'",
                })
                continue

            await session.execute(
                update(User)
                .where(User.username == username)
                .values(supervisor_id=username_to_id[sup_username])
            )

        await session.commit()

    errors.extend(supervisor_errors)

    return {
        "total_rows": total,
        "inserted": inserted,
        "failed": total - inserted,
        "errors": errors,
    }