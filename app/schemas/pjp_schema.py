from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, field_validator


class PJPRowSchema(BaseModel):
    username: str
    store_id: str
    date: Optional[str] = None
    is_active: bool = True

    @field_validator("username", mode="before")
    @classmethod
    def validate_username(cls, v):
        v = str(v).strip()
        if not v:
            raise ValueError("username is required")
        return v

    @field_validator("store_id", mode="before")
    @classmethod
    def validate_store_id(cls, v):
        v = str(v).strip()
        if not v:
            raise ValueError("store_id is required")
        return v

    @field_validator("date", mode="before")
    @classmethod
    def validate_date(cls, v):
        if v is None or str(v).strip() == "":
            return None
        v = str(v).strip()
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(v, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        raise ValueError(f"invalid date format: '{v}' (expected YYYY-MM-DD)")

    @field_validator("is_active", mode="before")
    @classmethod
    def parse_bool(cls, v):
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        if s == "true":
            return True
        if s == "false":
            return False
        raise ValueError(f"is_active must be True or False, got '{v}'")

    def get_date(self) -> Optional[date]:
        if self.date is None:
            return None
        return datetime.strptime(self.date, "%Y-%m-%d").date()