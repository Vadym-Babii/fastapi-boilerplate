from urllib.parse import urlparse
from arq.connections import RedisSettings

from app.core.config import settings
from app.workers.jobs import validate_addresses_batch, recognize_addresses_batch


def _redis_settings_from_url(url: str) -> RedisSettings:
    u = urlparse(url)
    db = int((u.path or "/0").lstrip("/"))
    return RedisSettings(
        host=u.hostname or "localhost",
        port=u.port or 6379,
        database=db,
        password=u.password,
        ssl=(u.scheme == "rediss"),
    )


class WorkerSettings:
    redis_settings = _redis_settings_from_url(settings.redis_url)
    functions = [validate_addresses_batch, recognize_addresses_batch]
    max_jobs = 10
