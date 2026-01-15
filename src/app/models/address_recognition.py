import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class AddressRecognitionBatch(Base):
    __tablename__ = "address_recognition_batches"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status: Mapped[str] = mapped_column(String(32), default="queued")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    request_payload: Mapped[list[dict] | None] = mapped_column(JSONB, nullable=True)


class AddressRecognitionItem(Base):
    __tablename__ = "address_recognition_items"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("address_recognition_batches.id", ondelete="CASCADE"), index=True
    )

    status: Mapped[str] = mapped_column(String(32), default="completed")
    recognized: Mapped[dict] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
