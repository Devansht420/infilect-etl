from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class PermanentJourneyPlan(Base):
    __tablename__ = "permanent_journey_plans"
    __table_args__ = (UniqueConstraint("user_id", "store_id", "date", name="uq_pjp_user_store_date"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    store_id: Mapped[int] = mapped_column(ForeignKey("stores.id", ondelete="CASCADE"), nullable=False)
    date: Mapped[date | None] = mapped_column(Date, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_on: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    modified_on: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())