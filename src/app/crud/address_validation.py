import uuid
from typing import Iterable

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.address_validation import AddressValidationBatch, AddressValidationItem


class AddressValidationCRUD:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_batch(
        self,
        *,
        status: str,
        request_payload: list[dict] | None = None,
    ) -> AddressValidationBatch:
        batch = AddressValidationBatch(status=status, request_payload=request_payload)
        self.session.add(batch)
        await self.session.flush()
        return batch

    async def get_batch(self, batch_id: uuid.UUID) -> AddressValidationBatch | None:
        return await self.session.get(AddressValidationBatch, batch_id)

    async def list_batches(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        status: str | None = None,
    ) -> list[tuple[AddressValidationBatch, int]]:
        stmt = (
            select(AddressValidationBatch, func.count(AddressValidationItem.id))
            .outerjoin(AddressValidationItem, AddressValidationItem.batch_id == AddressValidationBatch.id)
            .group_by(AddressValidationBatch.id)
            .order_by(AddressValidationBatch.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        if status:
            stmt = stmt.where(AddressValidationBatch.status == status)

        rows = (await self.session.execute(stmt)).all()
        return [(b, int(cnt)) for (b, cnt) in rows]

    async def get_batch_with_count(
        self, batch_id: uuid.UUID
    ) -> tuple[AddressValidationBatch, int] | None:
        stmt = (
            select(AddressValidationBatch, func.count(AddressValidationItem.id))
            .outerjoin(AddressValidationItem, AddressValidationItem.batch_id == AddressValidationBatch.id)
            .where(AddressValidationBatch.id == batch_id)
            .group_by(AddressValidationBatch.id)
        )
        row = (await self.session.execute(stmt)).one_or_none()
        if not row:
            return None
        batch, cnt = row
        return batch, int(cnt)

    async def clear_items(self, batch_id: uuid.UUID) -> None:
        await self.session.execute(
            delete(AddressValidationItem).where(AddressValidationItem.batch_id == batch_id)
        )

    async def delete_batch(self, batch_id: uuid.UUID) -> bool:
        batch = await self.session.get(AddressValidationBatch, batch_id)
        if not batch:
            return False
        self.session.delete(batch)
        return True
