from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

class DatabaseSessionManager:
    _engine: Optional[async_sessionmaker[AsyncSession]] = None
    _database_url: str = ""

    @classmethod
    def initialize(cls, database_url: str) -> None:
        cls._database_url = database_url
        engine = create_async_engine(database_url, pool_size=30, max_overflow=60, echo=False)
        cls._engine = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    @classmethod
    def get_session_factory(cls) -> async_sessionmaker[AsyncSession]:
        if cls._engine is None:
            raise RuntimeError("DatabaseSessionManager is not initialized")
        return cls._engine

    @classmethod
    async def dispose(cls) -> None:
        if cls._engine is not None:
            await cls._engine.close()  # type: ignore[union-attr]


@asynccontextmanager
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    factory = DatabaseSessionManager.get_session_factory()
    async with factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    factory = DatabaseSessionManager.get_session_factory()
    async with factory() as session:
        yield session


dispose_engine = DatabaseSessionManager.dispose
