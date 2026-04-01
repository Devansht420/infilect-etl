from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.lookup import City, Country, Region, State, StoreBrand, StoreType

LOOKUP_MODEL_MAP = {
    "store_brand": StoreBrand,
    "store_type": StoreType,
    "city": City,
    "state": State,
    "country": Country,
    "region": Region,
}


def normalize(value: str) -> str:
    return value.strip().title()


class LookupCache:
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
    name = normalize(raw_value)
    model = LOOKUP_MODEL_MAP[table]

    cached_id = cache.get(table, name)
    if cached_id is not None:
        return cached_id

    result = await session.execute(select(model).where(model.name == name))
    instance = result.scalar_one_or_none()

    if instance:
        cache.set(table, name, instance.id)
        return instance.id

    instance = model(name=name)
    session.add(instance)
    await session.flush()
    cache.set(table, name, instance.id)
    return instance.id