"""
User ingestion pipeline.

Special handling — supervisor self-reference:
  Users reference other users via supervisor_username. We can't insert a user
  whose supervisor hasn't been inserted yet. Solution: two-pass approach.

  Pass 1: Insert all valid users with supervisor_id = NULL.
  Pass 2: For each user that had a supervisor_username, look up the supervisor's
          DB id and UPDATE the row.

  This is simpler and more reliable than trying to sort rows topologically,
  especially since the file may have arbitrary ordering.
"""

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

    # Load existing usernames to catch duplicates against the DB
    existing_result = await session.execute(select(User.username))
    existing_usernames: set[str] = {row[0] for row in existing_result}
    seen_in_file: set[str] = set()

    # We read the whole file (30 rows, small) — chunking still used for consistency
    # For 500K users the same chunked approach works fine
    all_valid: list[dict[str, Any]] = []
    supervisor_map: dict[str, tuple[str, int]] = {}  # username → (supervisor_username, row_number)

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

            # Step 1 — Pydantic validation
            try:
                validated = UserRowSchema(**row_dict)
            except ValidationError as e:
                for err in e.errors():
                    field = err["loc"][0] if err["loc"] else "unknown"
                    errors.append({"row": abs_row, "column": field, "reason": err["msg"]})
                continue

            # Step 2 — Duplicate username check
            uname = validated.username
            if uname in existing_usernames or uname in seen_in_file:
                errors.append({
                    "row": abs_row,
                    "column": "username",
                    "reason": f"duplicate username '{uname}'",
                })
                continue
            seen_in_file.add(uname)

            # Step 3 — Validate supervisor_username exists (warn if not)
            if validated.supervisor_username:
                sup = validated.supervisor_username
                # We'll resolve after all users are inserted; just note it
                supervisor_map[uname] = (sup, abs_row)

            all_valid.append({
                "username": validated.username,
                "first_name": validated.first_name,
                "last_name": validated.last_name,
                "email": validated.email,
                "user_type": validated.user_type,
                "phone_number": validated.phone_number,
                "is_active": validated.is_active,
                "supervisor_id": None,  # filled in pass 2
            })

    # Pass 1 — bulk insert all valid users without supervisor_id
    if all_valid:
        for i in range(0, len(all_valid), settings.CHUNK_SIZE):
            chunk_data = all_valid[i : i + settings.CHUNK_SIZE]
            await session.execute(insert(User), chunk_data)
        await session.commit()
        inserted = len(all_valid)

    # Pass 2 — resolve supervisor FKs
    supervisor_errors = []
    if supervisor_map:
        # Fetch all newly inserted users' id + username in one query
        usernames_needing_supervisor = {sup for sup, _ in supervisor_map.values()}
        result = await session.execute(
            select(User.id, User.username).where(
                User.username.in_(usernames_needing_supervisor)
            )
        )
        username_to_id: dict[str, int] = {row.username: row.id for row in result}

        for username, (sup_username, row_num) in supervisor_map.items():
            if sup_username not in username_to_id:
                # Supervisor doesn't exist — record error but keep the user row
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