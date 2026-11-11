import pytest
pytest.importorskip("aiosqlite", reason="SQLite async test driver is a development dependency")
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database import get_session
from app.main import create_app


@pytest.mark.asyncio
async def test_create_get_list_cancel_api(test_engine):
    factory = async_sessionmaker(test_engine, expire_on_commit=False, autoflush=False)
    app = create_app()

    async def override_session():
        async with factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await client.post("/jobs", headers={"Idempotency-Key": "api-test-1"}, json={"payload": {"steps": 2}})
        assert created.status_code == 201
        job = created.json()
        repeated = await client.post("/jobs", headers={"Idempotency-Key": "api-test-1"}, json={"payload": {"steps": 99}})
        assert repeated.status_code == 200
        assert repeated.json()["id"] == job["id"]

        fetched = await client.get(f"/jobs/{job['id']}")
        assert fetched.status_code == 200
        assert fetched.json()["status"] == "queued"

        listed = await client.get("/jobs?limit=10&offset=0")
        assert listed.status_code == 200
        assert listed.json()["total"] == 1

        cancelled = await client.delete(f"/jobs/{job['id']}")
        assert cancelled.status_code == 200
        assert cancelled.json()["status"] == "cancelled"

        missing = await client.get("/jobs/00000000-0000-0000-0000-000000000001")
        assert missing.status_code == 404
    app.dependency_overrides.clear()
