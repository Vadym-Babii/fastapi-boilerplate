import uuid

from app.core.db.database import async_session_factory
from app.services.address_validation import AddressValidationService
from app.services.address_recognition import AddressRecognitionService


async def validate_addresses_batch(ctx, batch_id: str) -> None:
    service = AddressValidationService()
    batch_uuid = uuid.UUID(batch_id)

    async with async_session_factory() as session:
        await service.process_existing_batch(session, batch_uuid)

async def recognize_addresses_batch(ctx, batch_id: str) -> None:
    service = AddressRecognitionService()
    batch_uuid = uuid.UUID(batch_id)

    async with async_session_factory() as session:
        await service.process_existing_batch(session, batch_uuid)