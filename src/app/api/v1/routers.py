from fastapi import APIRouter

from app.api.v1.endpoints.addresses import router as addresses_router

router = APIRouter()
router.include_router(addresses_router)
