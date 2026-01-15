import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.db.database import get_db
from app.schemas.addresses import (
    AddressRecognizeIn,
    AddressRecognitionOut,
    AddressRecognizeResultOut,
    AddressIn,
    AddressOut,
    AddressValidationResultOut,
    ValidationBatchOut,
    BatchStatus,
)
from app.models.address_recognition import (
    AddressRecognitionBatch,
    AddressRecognitionItem,
)
from app.services.address_recognition import AddressRecognitionService
from app.services.address_validation import AddressValidationService


class AddressesAPI:
    def __init__(self) -> None:
        self.router = APIRouter(tags=["addresses"])
        self.service = AddressValidationService()
        self.recognition_service = AddressRecognitionService()
        self._register_routes()

    def _register_routes(self) -> None:
        self.router.post(
            "/v1/addresses/validate",
            response_model=list[AddressValidationResultOut],
            responses={
                status.HTTP_202_ACCEPTED: {
                    "description": (
                        "Accepted. Batch queued for background validation (ARQ). "
                        "Response body is empty list. Batch id returned in X-Validation-Batch-Id header."
                    )
                }
            },
        )(self.validate_addresses)

        self.router.get(
            "/v1/addresses/validate/{batch_id}",
            response_model=list[AddressValidationResultOut],
        )(self.get_validation_results)

        self.router.get(
            "/v1/validation-batches",
            response_model=list[ValidationBatchOut],
        )(self.list_validation_batches)

        self.router.get(
            "/v1/validation-batches/{batch_id}",
            response_model=ValidationBatchOut,
        )(self.get_validation_batch)

        self.router.delete(
            "/v1/validation-batches/{batch_id}",
            status_code=status.HTTP_204_NO_CONTENT,
        )(self.delete_validation_batch)

        self.router.post(
            "/v1/validation-batches/{batch_id}/requeue",
            status_code=status.HTTP_202_ACCEPTED,
        )(self.requeue_validation_batch)
        
        self.router.put(
            "/v1/addresses/recognize",
            response_model=list[AddressRecognizeResultOut],
            responses={
                status.HTTP_202_ACCEPTED: {"description": "Accepted. Recognition queued (ARQ)."}
            },
        )(self.recognize_addresses)
        
        self.router.get(
            "/v1/addresses/recognize/{recognition_id}",
            response_model=list[AddressRecognizeResultOut],
        )(self.get_recognition_results)

    async def validate_addresses(
        self,
        request: Request,
        response: Response,
        addresses: list[AddressIn],
        async_mode: bool = Query(False, alias="async"),
        db: AsyncSession = Depends(get_db),
    ) -> list[AddressValidationResultOut]:
        if async_mode:
            batch_id = await self.service.create_queued_batch(db, addresses)
            await request.app.state.redis.enqueue_job("validate_addresses_batch", str(batch_id))

            response.status_code = status.HTTP_202_ACCEPTED
            response.headers["X-Validation-Batch-Id"] = str(batch_id)
            return []

        batch_id, results = await self.service.validate_and_store(db, addresses)
        response.headers["X-Validation-Batch-Id"] = str(batch_id)
        return results

    async def get_validation_results(
        self,
        batch_id: uuid.UUID,
        db: AsyncSession = Depends(get_db),
    ) -> list[AddressValidationResultOut]:
        results = await self.service.get_batch_results(db, batch_id)
        if not results:
            raise HTTPException(status_code=404, detail="batch_id not found or empty")
        return results

    async def list_validation_batches(
        self,
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
        status_filter: BatchStatus | None = Query(None, alias="status"),
        db: AsyncSession = Depends(get_db),
    ) -> list[ValidationBatchOut]:
        return await self.service.list_batches(
            db,
            limit=limit,
            offset=offset,
            status=status_filter,
        )

    async def get_validation_batch(
        self,
        batch_id: uuid.UUID,
        db: AsyncSession = Depends(get_db),
    ) -> ValidationBatchOut:
        batch = await self.service.get_batch(db, batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail="batch_id not found")
        return batch

    async def delete_validation_batch(
        self,
        batch_id: uuid.UUID,
        db: AsyncSession = Depends(get_db),
    ) -> None:
        ok = await self.service.delete_batch(db, batch_id)
        if not ok:
            raise HTTPException(status_code=404, detail="batch_id not found")

    async def requeue_validation_batch(
        self,
        request: Request,
        batch_id: uuid.UUID,
        db: AsyncSession = Depends(get_db),
    ) -> None:
        try:
            ok = await self.service.requeue_batch(db, batch_id)
        except RuntimeError:
            raise HTTPException(status_code=409, detail="batch is processing")

        if not ok:
            raise HTTPException(status_code=404, detail="batch_id not found")

        await request.app.state.redis.enqueue_job("validate_addresses_batch", str(batch_id))
        
    async def recognize_addresses(
        self,
        request: Request,
        response: Response,
        payload: list[AddressRecognizeIn],
        async_mode: bool = Query(False, alias="async"),
        db: AsyncSession = Depends(get_db),
    ) -> list[AddressRecognizeResultOut]:
        if async_mode:
            rec_id = await self.recognition_service.create_queued_batch(db, payload)
            await request.app.state.redis.enqueue_job(
                "recognize_addresses_batch",
                str(rec_id),
            )

            response.status_code = status.HTTP_202_ACCEPTED
            response.headers["X-Recognition-Id"] = str(rec_id)
            return []

        rec_id, results = await self.recognition_service.recognize_and_store(db, payload)
        response.headers["X-Recognition-Id"] = str(rec_id)
        return results
    
    async def get_recognition_results(
        self,
        recognition_id: uuid.UUID,
        db: AsyncSession = Depends(get_db),
    ) -> list[AddressRecognizeResultOut]:
        stmt = (
            select(AddressRecognitionItem)
            .where(AddressRecognitionItem.batch_id == recognition_id)
            .order_by(AddressRecognitionItem.created_at)
        )

        rows = (await db.execute(stmt)).scalars().all()

        if not rows:
            raise HTTPException(status_code=404, detail="recognition_id not found or empty")

        results: list[AddressRecognizeResultOut] = []

        for r in rows:
            blob = r.recognized or {}

            results.append(
                AddressRecognizeResultOut(
                    status="recognized",
                    original_address=blob.get("original_address") or {},
                    recognized_address=blob.get("recognized_address") or {},
                )
            )

        return results




addresses_api = AddressesAPI()
router = addresses_api.router
