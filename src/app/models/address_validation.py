import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class AddressValidationBatch(Base):
    __tablename__ = "address_validation_batches"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status: Mapped[str] = mapped_column(String(32), default="completed")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    request_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)


class AddressValidationItem(Base):
    __tablename__ = "address_validation_items"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("address_validation_batches.id", ondelete="CASCADE"), index=True
    )

    status: Mapped[str] = mapped_column(String(32))
    original_address: Mapped[dict] = mapped_column(JSONB)
    matched_address: Mapped[dict] = mapped_column(JSONB)
    messages: Mapped[list] = mapped_column(JSONB, default=list)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
