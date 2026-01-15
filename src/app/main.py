from contextlib import asynccontextmanager
from fastapi import FastAPI
from arq.connections import create_pool, RedisSettings

from app.core.config import settings
from app.api.v1.routers import router as v1_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = await create_pool(RedisSettings.from_dsn(settings.redis_url))
    try:
        yield
    finally:
        await app.state.redis.close()

def create_app() -> FastAPI:
    app = FastAPI(title="FastAPI ShipEngine Service", lifespan=lifespan)
    app.include_router(v1_router)
    return app

app = create_app()
