import uuid
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.address_recognition import AddressRecognitionBatch, AddressRecognitionItem
from app.schemas.addresses import AddressIn, AddressOut, AddressRecognizeResultOut, AddressRecognizeIn


class AddressRecognitionService:
    def _recognize_one(self, addr: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}

        for k, v in addr.items():
            if isinstance(v, str):
                vv = v.strip()
                if k == "country_code":
                    out[k] = vv.upper()
                elif k == "email":
                    out[k] = vv.lower()
                elif k == "address_residential_indicator":
                    x = vv.strip().lower()
                    out[k] = x if x in {"unknown", "yes", "no"} else "unknown"
                else:
                    out[k] = vv.upper()
            else:
                out[k] = v

        if "address_residential_indicator" not in out or out["address_residential_indicator"] is None:
            out["address_residential_indicator"] = "unknown"

        return out

    async def create_queued_batch(self, session: AsyncSession, addresses: list[AddressIn]) -> uuid.UUID:
        payload = [a.model_dump() for a in addresses]
        batch = AddressRecognitionBatch(status="queued", request_payload=payload)
        session.add(batch)
        await session.flush()
        await session.commit()
        return batch.id

    async def recognize_and_store(
        self, session: AsyncSession, addresses: list[AddressIn]
    ) -> tuple[uuid.UUID, list[AddressRecognizeResultOut]]:
        payload = [a.model_dump() for a in addresses]
        batch = AddressRecognitionBatch(status="completed", request_payload=payload)
        session.add(batch)
        await session.flush()

        results: list[AddressRecognizeResultOut] = []

        for addr in addresses:
            original = addr.model_dump()
            recognized = self._recognize_one(original)

            session.add(
                AddressRecognitionItem(
                    batch_id=batch.id,
                    status="completed",
                    recognized={
                        "original_address": original,
                        "recognized_address": recognized,
                    },
                )
            )

            results.append(
                AddressRecognizeResultOut(
                    original_address=AddressOut.model_validate(original),
                    recognized_address=AddressOut.model_validate(recognized),
                )
            )

        await session.commit()
        return batch.id, results

    async def process_existing_batch(self, session: AsyncSession, batch_id: uuid.UUID) -> None:
        batch = await session.get(AddressRecognitionBatch, batch_id)
        if batch is None:
            return

        if batch.status in {"processing", "completed"}:
            return

        payload = batch.request_payload or []
        if not payload:
            batch.status = "failed"
            await session.commit()
            return

        batch.status = "processing"
        await session.commit()

        await session.execute(delete(AddressRecognitionItem).where(AddressRecognitionItem.batch_id == batch_id))

        addresses = [AddressRecognizeIn.model_validate(x) for x in payload]
        for addr in addresses:
            original = addr.address.model_dump() if addr.address else {}
            recognized = self._recognize_one(original)
            session.add(
                AddressRecognitionItem(
                    batch_id=batch_id,
                    status="completed",
                    recognized={
                        "original_address": original,
                        "recognized_address": recognized,
                    },
                )
            )

        b2 = await session.get(AddressRecognitionBatch, batch_id)
        if b2 is not None:
            b2.status = "completed"

        await session.commit()

    async def get_results(self, session: AsyncSession, batch_id: uuid.UUID) -> list[AddressRecognizeResultOut]:
        stmt = (
            select(AddressRecognitionItem)
            .where(AddressRecognitionItem.batch_id == batch_id)
            .order_by(AddressRecognitionItem.created_at)
        )
        rows = (await session.execute(stmt)).scalars().all()

        out: list[AddressRecognizeResultOut] = []
        for r in rows:
            blob = r.recognized or {}
            out.append(
                AddressRecognizeResultOut(
                    original_address=AddressOut.model_validate(blob.get("original_address") or {}),
                    recognized_address=AddressOut.model_validate(blob.get("recognized_address") or {}),
                )
            )
        return out
