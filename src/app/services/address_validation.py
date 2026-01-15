import uuid
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.address_validation import AddressValidationBatch, AddressValidationItem
from app.schemas.addresses import (
    AddressIn,
    AddressOut,
    AddressValidationResultOut,
    ValidationBatchOut,
    ValidationMessage,
)


class AddressValidationService:
    def _normalize(self, addr: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}

        for key, value in addr.items():
            if isinstance(value, str):
                v = value.strip()

                if key == "address_residential_indicator":
                    v = v.lower()
                    if v not in {"unknown", "yes", "no"}:
                        v = "unknown"
                    normalized[key] = v
                    continue

                if key == "country_code":
                    normalized[key] = v.upper()
                    continue

                if key == "email":
                    normalized[key] = v.lower()
                    continue

                normalized[key] = v.upper()
            else:
                normalized[key] = value

        if "address_residential_indicator" not in normalized:
            normalized["address_residential_indicator"] = "unknown"

        return normalized

    def _status_and_messages(self, addr: AddressIn) -> tuple[str, list[ValidationMessage]]:
        msgs: list[ValidationMessage] = []

        if addr.country_code.upper() == "US" and not addr.postal_code:
            msgs.append(
                ValidationMessage(
                    code="missing_postal_code",
                    message="postal_code is recommended for US",
                    level="warning",
                )
            )

        return "verified", msgs

    async def create_queued_batch(self, session: AsyncSession, addresses: list[AddressIn]) -> uuid.UUID:
        payload = [a.model_dump() for a in addresses]

        async with session.begin():
            batch = AddressValidationBatch(status="queued", request_payload=payload)
            session.add(batch)
            await session.flush()

        return batch.id

    async def validate_and_store(
        self,
        session: AsyncSession,
        addresses: list[AddressIn],
    ) -> tuple[uuid.UUID, list[AddressValidationResultOut]]:
        results: list[AddressValidationResultOut] = []
        payload = [a.model_dump() for a in addresses]

        async with session.begin():
            batch = AddressValidationBatch(status="completed", request_payload=payload)
            session.add(batch)
            await session.flush()

            for addr in addresses:
                original_dict = addr.model_dump()
                matched_dict = self._normalize(original_dict)

                status, messages = self._status_and_messages(addr)
                messages_json = [m.model_dump() for m in messages]

                session.add(
                    AddressValidationItem(
                        batch_id=batch.id,
                        status=status,
                        original_address=original_dict,
                        matched_address=matched_dict,
                        messages=messages_json,
                    )
                )

                original_for_out = dict(original_dict)
                ari = original_for_out.get("address_residential_indicator")
                if isinstance(ari, str):
                    v = ari.strip().lower()
                    original_for_out["address_residential_indicator"] = (
                        v if v in {"unknown", "yes", "no"} else "unknown"
                    )
                elif ari is None:
                    original_for_out["address_residential_indicator"] = "unknown"

                results.append(
                    AddressValidationResultOut(
                        status=status,
                        original_address=AddressOut.model_validate(original_for_out),
                        matched_address=AddressOut.model_validate(matched_dict),
                        messages=messages,
                    )
                )

        return batch.id, results

    async def process_existing_batch(self, session: AsyncSession, batch_id: uuid.UUID) -> None:
        async with session.begin():
            batch = (
                await session.execute(
                    select(AddressValidationBatch)
                    .where(AddressValidationBatch.id == batch_id)
                    .with_for_update()
                )
            ).scalars().first()

            if batch is None:
                return

            existing_item_id = (
                await session.execute(
                    select(AddressValidationItem.id)
                    .where(AddressValidationItem.batch_id == batch_id)
                    .limit(1)
                )
            ).scalar_one_or_none()

            if batch.status == "completed" and existing_item_id is not None:
                return

            payload = batch.request_payload or []
            if not payload:
                batch.status = "failed"
                return

            batch.status = "processing"

            await session.execute(delete(AddressValidationItem).where(AddressValidationItem.batch_id == batch_id))

            addresses = [AddressIn.model_validate(a) for a in payload]

            for addr in addresses:
                original_dict = addr.model_dump()
                matched_dict = self._normalize(original_dict)
                status, messages = self._status_and_messages(addr)

                session.add(
                    AddressValidationItem(
                        batch_id=batch_id,
                        status=status,
                        original_address=original_dict,
                        matched_address=matched_dict,
                        messages=[m.model_dump() for m in messages],
                    )
                )

            batch.status = "completed"

    async def get_batch_results(self, session: AsyncSession, batch_id: uuid.UUID) -> list[AddressValidationResultOut]:
        q = (
            select(AddressValidationItem)
            .where(AddressValidationItem.batch_id == batch_id)
            .order_by(AddressValidationItem.created_at)
        )
        rows = (await session.execute(q)).scalars().all()

        out: list[AddressValidationResultOut] = []
        for r in rows:
            out.append(
                AddressValidationResultOut(
                    status=r.status,
                    original_address=r.original_address,
                    matched_address=r.matched_address,
                    messages=[ValidationMessage.model_validate(m) for m in (r.messages or [])],
                )
            )

        return out

    async def list_batches(
        self,
        session: AsyncSession,
        *,
        limit: int = 50,
        offset: int = 0,
        status: str | None = None,
    ) -> list[ValidationBatchOut]:
        stmt = (
            select(
                AddressValidationBatch,
                func.count(AddressValidationItem.id).label("items_count"),
            )
            .outerjoin(AddressValidationItem, AddressValidationItem.batch_id == AddressValidationBatch.id)
            .group_by(AddressValidationBatch.id)
            .order_by(AddressValidationBatch.created_at.desc())
            .limit(limit)
            .offset(offset)
        )

        if status:
            stmt = stmt.where(AddressValidationBatch.status == status)

        rows = (await session.execute(stmt)).all()

        return [
            ValidationBatchOut(
                id=b.id,
                status=b.status,
                created_at=b.created_at,
                items_count=int(cnt or 0),
                request_payload=b.request_payload,
            )
            for (b, cnt) in rows
        ]

    async def get_batch(
        self,
        session: AsyncSession,
        batch_id: uuid.UUID,
    ) -> ValidationBatchOut | None:
        stmt = (
            select(
                AddressValidationBatch,
                func.count(AddressValidationItem.id).label("items_count"),
            )
            .outerjoin(AddressValidationItem, AddressValidationItem.batch_id == AddressValidationBatch.id)
            .where(AddressValidationBatch.id == batch_id)
            .group_by(AddressValidationBatch.id)
        )

        row = (await session.execute(stmt)).first()
        if row is None:
            return None

        b, cnt = row
        return ValidationBatchOut(
            id=b.id,
            status=b.status,
            created_at=b.created_at,
            items_count=int(cnt or 0),
            request_payload=b.request_payload,
        )

    async def delete_batch(self, session: AsyncSession, batch_id: uuid.UUID) -> bool:
        async with session.begin():
            batch = await session.get(AddressValidationBatch, batch_id)
            if batch is None:
                return False
            await session.execute(delete(AddressValidationItem).where(AddressValidationItem.batch_id == batch_id))
            await session.delete(batch)
        return True

    async def requeue_batch(self, session: AsyncSession, batch_id: uuid.UUID) -> bool:
        async with session.begin():
            batch = (
                await session.execute(
                    select(AddressValidationBatch)
                    .where(AddressValidationBatch.id == batch_id)
                    .with_for_update()
                )
            ).scalars().first()

            if batch is None:
                return False

            if batch.status == "processing":
                raise RuntimeError("processing")

            if not batch.request_payload:
                batch.status = "failed"
                return False

            batch.status = "queued"
            await session.execute(
                delete(AddressValidationItem).where(AddressValidationItem.batch_id == batch_id)
            )

        return True
