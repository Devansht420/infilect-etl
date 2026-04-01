"""
Lookup service — get_or_create for all 6 lookup tables.

Key design decisions:
- Normalize all values: strip whitespace + title-case before any DB hit.
  This prevents "Star Bazaar" / "  star bazaar  " / "STAR BAZAAR" creating 3 rows.
- In-memory cache (dict) per ingestion job.
  For a 500K file, the same city/brand appears thousands of times —
  we resolve it from cache instead of hitting the DB every row.
- flush() not commit() after insert — keeps the new id available within
  the transaction without committing prematurely.
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.lookup import City, Country, Region, State, StoreBrand, StoreType

# Map column name → SQLAlchemy model
LOOKUP_MODEL_MAP = {
    "store_brand": StoreBrand,
    "store_type": StoreType,
    "city": City,
    "state": State,
    "country": Country,
    "region": Region,
}


def normalize(value: str) -> str:
    """Strip whitespace and apply title-case for consistent storage."""
    return value.strip().title()


class LookupCache:
    """
    Per-request in-memory cache for lookup table IDs.
    Structure: { "cities": {"Chennai": 1, "Delhi": 2, ...}, ... }
    """

    def __init__(self):
        self._cache: dict[str, dict[str, int]] = {
            "store_brand": {},
            "store_type": {},
            "city": {},
            "state": {},
            "country": {},
            "region": {},
        }

    def get(self, table: str, name: str) -> int | None:
        return self._cache[table].get(name)

    def set(self, table: str, name: str, id_: int):
        self._cache[table][name] = id_


async def get_or_create_lookup(
    session: AsyncSession,
    cache: LookupCache,
    table: str,
    raw_value: str,
) -> int:
    """
    Resolve a lookup value to an ID.
    1. Normalize the value.
    2. Check in-memory cache — return immediately if found.
    3. Query DB — if found, cache and return.
    4. If not found — insert, flush to get ID, cache and return.
    """
    name = normalize(raw_value)
    model = LOOKUP_MODEL_MAP[table]

    # 1. Cache hit
    cached_id = cache.get(table, name)
    if cached_id is not None:
        return cached_id

    # 2. DB lookup
    result = await session.execute(select(model).where(model.name == name))
    instance = result.scalar_one_or_none()

    if instance:
        cache.set(table, name, instance.id)
        return instance.id

    # 3. Create new
    instance = model(name=name)
    session.add(instance)
    await session.flush()  # assigns instance.id without full commit
    cache.set(table, name, instance.id)
    return instance.id