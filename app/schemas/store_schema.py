"""
Pydantic schema for one row of stores_master.csv.

Intentional errors found in the file (caught by these validators):
- Row 6  : duplicate store_id STR-0004
- Row 12 : missing store_id (empty string)
- Row 18 : latitude = "not_available" (not a float)
- Row 23 : longitude = 999.999 (out of valid range -180..180)
- Row 44 : store_id / store_brand / city have leading+trailing whitespace (normalized, not rejected)
- Row 49 : name is 300+ characters (exceeds VARCHAR(255))
- Row 54 : title is empty string
- Row 59 : country = "india" (lowercase — normalized to "India", not rejected)
- Row 76 : latitude = -999 (out of valid range -90..90)
- Row 86 : store_id = "12345" (does not match STR-XXXX pattern)
- Row 91 : title = "   " (whitespace-only, treated as empty)
"""

import re
from typing import Optional

from pydantic import BaseModel, field_validator, model_validator


STORE_ID_PATTERN = re.compile(r"^STR-\d{4,}$")


class StoreRowSchema(BaseModel):
    store_id: str
    store_external_id: Optional[str] = ""
    name: str
    title: str
    store_brand: str
    store_type: str
    city: str
    state: str
    country: str
    region: str
    latitude: float
    longitude: float

    @field_validator("store_id", mode="before")
    @classmethod
    def validate_store_id(cls, v):
        v = str(v).strip()
        if not v:
            raise ValueError("store_id is required")
        if not STORE_ID_PATTERN.match(v):
            raise ValueError(f"store_id must match STR-XXXX format, got '{v}'")
        return v

    @field_validator("name", "title", mode="before")
    @classmethod
    def validate_required_str(cls, v):
        v = str(v).strip()
        if not v:
            raise ValueError("field is required and cannot be blank")
        return v

    @field_validator("name", mode="before")
    @classmethod
    def validate_name_length(cls, v):
        v = str(v).strip()
        if len(v) > 255:
            raise ValueError(f"name exceeds 255 characters (got {len(v)})")
        return v

    @field_validator("store_brand", "store_type", "city", "state", "country", "region", mode="before")
    @classmethod
    def strip_lookup_fields(cls, v):
        """Strip whitespace — normalization (title-case) happens in lookup_service."""
        v = str(v).strip()
        if not v:
            raise ValueError("field is required")
        return v

    @field_validator("latitude", mode="before")
    @classmethod
    def validate_latitude(cls, v):
        try:
            v = float(str(v).strip())
        except (ValueError, TypeError):
            raise ValueError(f"latitude must be a number, got '{v}'")
        if not (-90 <= v <= 90):
            raise ValueError(f"latitude must be between -90 and 90, got {v}")
        return v

    @field_validator("longitude", mode="before")
    @classmethod
    def validate_longitude(cls, v):
        try:
            v = float(str(v).strip())
        except (ValueError, TypeError):
            raise ValueError(f"longitude must be a number, got '{v}'")
        if not (-180 <= v <= 180):
            raise ValueError(f"longitude must be between -180 and 180, got {v}")
        return v