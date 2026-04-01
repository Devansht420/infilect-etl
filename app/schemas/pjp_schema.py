"""
Pydantic schema for one row of store_user_mapping.csv.

Intentional errors found in the file:
- Row 11 : username = "nonexistent.user" — not in users table
- Row 12 : username = "phantom.person" — not in users table
- Row 21 : store_id = "STR-9999" — not in stores table
- Row 22 : store_id = "INVALID-STORE" — not in stores table + bad format
- Row 31 : date = "32-13-2025" — invalid date
- Row 32 : date = "not-a-date" — invalid date
- Row 41 : duplicate row (rahul.patel26, STR-0031, 2026-01-10) — same as row 40
- Row 51 : store_id is empty
- Row 61 : username is empty
- Row 71 : date = "2099-12-31" — suspiciously far future (warn, not reject)
- Row 84 : is_active = "maybe" — not a valid boolean
"""

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, field_validator


class PJPRowSchema(BaseModel):
    username: str
    store_id: str
    # Declared as Optional[str] so Pydantic v2 doesn't try to coerce the
    # raw CSV string into a date before our validator runs.
    # Our validator handles the string → date conversion explicitly.
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
        """Return the date field as a real date object for DB insertion."""
        if self.date is None:
            return None
        return datetime.strptime(self.date, "%Y-%m-%d").date()