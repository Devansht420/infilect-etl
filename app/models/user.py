from datetime import datetime

from sqlalchemy import Boolean, CheckConstraint, DateTime, ForeignKey, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(150), unique=True, nullable=False)
    first_name: Mapped[str] = mapped_column(String(150), default="")
    last_name: Mapped[str] = mapped_column(String(150), default="")
    email: Mapped[str] = mapped_column(String(254), nullable=False)
    user_type: Mapped[int] = mapped_column(
        Integer,
        CheckConstraint("user_type IN (1, 2, 3, 7)", name="valid_user_type"),
        default=1,
    )
    phone_number: Mapped[str] = mapped_column(String(32), default="")
    supervisor_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_on: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    modified_on: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())