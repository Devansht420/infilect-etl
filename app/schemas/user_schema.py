"""
Pydantic schema for one row of users_master.csv.

Intentional errors found in the file:
- Row 8  : duplicate username neha.sharma6
- Row 13 : user_type = 99 (not in allowed set 1,2,3,7)
- Row 17 : phone_number = "+91ABC1234567" (invalid format)
- Row 19 : supervisor_username = "anita.singh56" — username doesn't exist in the file
           (anita.singh56 is the email prefix of row 8, but the username there is neha.sharma6)
- Row 21 : supervisor_username = "ghost.user42" — does not exist at all
- Row 25 : username is empty
- Row 28 : email = "not-an-email" (invalid format)
"""

import re
from typing import Optional

from pydantic import BaseModel, field_validator

VALID_USER_TYPES = {1, 2, 3, 7}
PHONE_PATTERN = re.compile(r"^\+?[\d\s\-().]{7,20}$")


class UserRowSchema(BaseModel):
    username: str
    first_name: Optional[str] = ""
    last_name: Optional[str] = ""
    email: str
    user_type: int
    phone_number: Optional[str] = ""
    supervisor_username: Optional[str] = None  # resolved to supervisor_id during ingestion
    is_active: bool = True

    @field_validator("username", mode="before")
    @classmethod
    def validate_username(cls, v):
        v = str(v).strip()
        if not v:
            raise ValueError("username is required")
        if len(v) > 150:
            raise ValueError(f"username exceeds 150 characters")
        return v

    @field_validator("email", mode="before")
    @classmethod
    def validate_email(cls, v):
        v = str(v).strip()
        # Basic RFC check: must have exactly one @, a dot after @, reasonable length
        if not v:
            raise ValueError("email is required")
        if len(v) > 254:
            raise ValueError("email exceeds 254 characters")
        parts = v.split("@")
        if len(parts) != 2 or "." not in parts[1]:
            raise ValueError(f"invalid email format: '{v}'")
        return v

    @field_validator("user_type", mode="before")
    @classmethod
    def validate_user_type(cls, v):
        try:
            v = int(str(v).strip())
        except (ValueError, TypeError):
            raise ValueError(f"user_type must be an integer, got '{v}'")
        if v not in VALID_USER_TYPES:
            raise ValueError(f"user_type must be one of {VALID_USER_TYPES}, got {v}")
        return v

    @field_validator("phone_number", mode="before")
    @classmethod
    def validate_phone(cls, v):
        if v is None or str(v).strip() == "":
            return ""
        v = str(v).strip()
        if not PHONE_PATTERN.match(v):
            raise ValueError(f"invalid phone number format: '{v}'")
        if len(v) > 32:
            raise ValueError(f"phone_number exceeds 32 characters")
        return v

    @field_validator("supervisor_username", mode="before")
    @classmethod
    def clean_supervisor(cls, v):
        if v is None or str(v).strip() == "":
            return None
        return str(v).strip()

    @field_validator("is_active", mode="before")
    @classmethod
    def parse_bool(cls, v):
        if isinstance(v, bool):
            return v
        if str(v).strip().lower() == "true":
            return True
        if str(v).strip().lower() == "false":
            return False
        raise ValueError(f"is_active must be True or False, got '{v}'")

    @field_validator("first_name", "last_name", mode="before")
    @classmethod
    def clean_name(cls, v):
        if v is None:
            return ""
        return str(v).strip()[:150]