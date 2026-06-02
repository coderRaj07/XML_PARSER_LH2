from collections.abc import AsyncGenerator
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.infrastructure.db.models import Base


class DatabaseSessionManager:
    _engine: Optional[async_sessionmaker[AsyncSession]] = None
    _database_url: str = ""

    @classmethod
    def initialize(cls, database_url: str) -> None:
        cls._database_url = database_url
        engine = create_async_engine(database_url, pool_size=10, max_overflow=20, echo=False)
        cls._engine = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    @classmethod
    def get_session_factory(cls) -> async_sessionmaker[AsyncSession]:
        if cls._engine is None:
            raise RuntimeError("DatabaseSessionManager is not initialized")
        return cls._engine

    @classmethod
    async def create_tables(cls) -> None:
        if not cls._database_url:
            return
        engine = create_async_engine(cls._database_url)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await engine.dispose()

    @classmethod
    async def dispose(cls) -> None:
        if cls._engine is not None:
            await cls._engine.close()  # type: ignore[union-attr]


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    factory = DatabaseSessionManager.get_session_factory()
    async with factory() as session:
        yield session


create_tables = DatabaseSessionManager.create_tables
dispose_engine = DatabaseSessionManager.dispose
