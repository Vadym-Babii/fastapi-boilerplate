import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

BatchStatus = Literal["queued", "processing", "completed", "failed"]
ResidentialIndicator = Literal["unknown", "yes", "no"]
ValidationStatus = Literal["verified", "unverified", "error"]
ValidationLevel = Literal["info", "warning", "error"]


class AddressIn(BaseModel):
    name: str | None = None
    phone: str | None = None
    email: str | None = None
    company_name: str | None = None

    address_line1: str = Field(min_length=1)
    address_line2: str | None = None
    address_line3: str | None = None

    city_locality: str = Field(min_length=1)
    state_province: str = Field(min_length=1)
    postal_code: str | int | None = None
    country_code: str = Field(min_length=2, max_length=2)

    address_residential_indicator: ResidentialIndicator = "unknown"

    @field_validator("address_residential_indicator", mode="before")
    @classmethod
    def normalize_residential_indicator(cls, v: Any) -> str:
        if v is None:
            return "unknown"
        if isinstance(v, str):
            vv = v.strip().lower()
            if vv in {"unknown", "yes", "no"}:
                return vv
        return "unknown"


class AddressOut(AddressIn):
    pass


class ValidationMessage(BaseModel):
    code: str
    message: str
    level: ValidationLevel = "info"


class AddressValidationResultOut(BaseModel):
    status: ValidationStatus
    original_address: AddressOut
    matched_address: AddressOut
    messages: list[ValidationMessage] = Field(default_factory=list)


class ValidationBatchOut(BaseModel):
    id: uuid.UUID
    status: BatchStatus
    created_at: datetime
    items_count: int = 0
    request_payload: list[dict] | None = None


class AddressRecognizeKnownValues(BaseModel):
    name: str | None = None
    address_line1: str | None = None
    address_line2: str | None = None
    address_line3: str | None = None
    city_locality: str | None = None
    state_province: str | None = None
    postal_code: str | int | None = None
    country_code: str | None = None
    address_residential_indicator: ResidentialIndicator | None = None


class AddressRecognizeIn(BaseModel):
    text: str = Field(min_length=1)
    address: AddressRecognizeKnownValues | None = None


class RecognizedEntityOut(BaseModel):
    type: str
    score: float
    text: str | int
    start_index: int
    end_index: int
    result: dict[str, Any]


class AddressRecognitionOut(BaseModel):
    score: float
    address: dict[str, Any]
    entities: list[RecognizedEntityOut]


class PartialAddressOut(BaseModel):
    name: str | None = None
    phone: str | None = None
    email: str | None = None
    company_name: str | None = None

    address_line1: str | None = None
    address_line2: str | None = None
    address_line3: str | None = None
    city_locality: str | None = None
    state_province: str | None = None
    postal_code: str | int | None = None
    country_code: str | None = None
    address_residential_indicator: ResidentialIndicator | None = None


class AddressRecognizeResultOut(BaseModel):
    status: Literal["recognized", "error"] = "recognized"
    original_address: PartialAddressOut
    recognized_address: PartialAddressOut
