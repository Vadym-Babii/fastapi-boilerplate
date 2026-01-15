# FastAPI Async Microservice Boilerplate

Production-style **FastAPI** boilerplate with **async SQLAlchemy + PostgreSQL** and background jobs via **ARQ (Redis)**.

This template is intended to be a reusable starting point for async microservices:
- clean project structure
- async end-to-end request handling
- transactional DB operations
- background job processing
- batch-style APIs (sync + async patterns)

---

## 🧱 Tech Stack

- Python 3.x
- FastAPI
- SQLAlchemy 2.x (async) + asyncpg
- PostgreSQL
- Alembic
- Redis
- ARQ (async task queue)
- Docker / Docker Compose

---

## 🗂 Project Structure (simplified)

```text
src/
├── app/
│   ├── api/v1/endpoints/     # route handlers
│   ├── services/             # business logic
│   ├── models/               # ORM models
│   ├── schemas/              # Pydantic schemas
│   ├── crud/                 # DB helpers (optional)
│   ├── workers/              # ARQ worker + jobs
│   └── core/config.py        # settings & environment
├── alembic/                  # database migrations
└── main.py
```

---

## 🚀 Run locally

### 1) Environment

Create `.env` from sample:

```bash
cp .env.sample .env
```

Example `.env`:

```env
ENVIRONMENT=local

POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_SERVER=postgres
POSTGRES_PORT=5432
POSTGRES_DB=app

REDIS_URL=redis://redis:6379/0
SQL_ECHO=false
```

### 2) Docker

```bash
docker compose up --build
```

What happens on startup:
- PostgreSQL & Redis containers start
- Alembic migrations are applied automatically (if configured that way)
- FastAPI API starts on port `8000`
- ARQ worker starts in a separate container

---

## 📖 API Docs

Swagger UI:

```text
http://localhost:8000/docs
```

---

## 🧩 Notes

This repository is a **boilerplate**. Any existing endpoints and flows are included as examples and can be replaced/extended for your own domain.

---

## Use as a template

1. Rename project/package names.
2. Replace example endpoints with your domain modules.
3. Update `.env.sample`.
4. Add CI, tests, and linting rules that match your workflow.
