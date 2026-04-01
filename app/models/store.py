from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Store(Base):
    __tablename__ = "stores"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    store_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    store_external_id: Mapped[str] = mapped_column(String(255), default="")
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)

    store_brand_id: Mapped[int | None] = mapped_column(ForeignKey("store_brands.id", ondelete="SET NULL"), nullable=True)
    store_type_id: Mapped[int | None] = mapped_column(ForeignKey("store_types.id", ondelete="SET NULL"), nullable=True)
    city_id: Mapped[int | None] = mapped_column(ForeignKey("cities.id", ondelete="SET NULL"), nullable=True)
    state_id: Mapped[int | None] = mapped_column(ForeignKey("states.id", ondelete="SET NULL"), nullable=True)
    country_id: Mapped[int | None] = mapped_column(ForeignKey("countries.id", ondelete="SET NULL"), nullable=True)
    region_id: Mapped[int | None] = mapped_column(ForeignKey("regions.id", ondelete="SET NULL"), nullable=True)

    latitude: Mapped[float] = mapped_column(Float, default=0.0)
    longitude: Mapped[float] = mapped_column(Float, default=0.0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_on: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    modified_on: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())