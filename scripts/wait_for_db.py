import asyncio
import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.config import settings
from app.database import engine


async def main() -> None:
    logger = logging.getLogger("wait_for_db")
    for attempt in range(1, 61):
        try:
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
            print("Database is ready")
            return
        except SQLAlchemyError as exc:
            logger.warning("Database not ready (attempt %s/60): %s", attempt, exc)
            await asyncio.sleep(2)
    raise RuntimeError(f"Database did not become ready: {settings.database_url.split('@')[-1]}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
