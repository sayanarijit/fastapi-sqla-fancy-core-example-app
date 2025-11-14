from contextlib import asynccontextmanager

from sqla_fancy_core import fancy
from sqlalchemy.ext.asyncio import create_async_engine

from tables import tb

engine = create_async_engine("sqlite+aiosqlite:///./test.db")
# engine = create_async_engine("postgresql+asyncpg://test:test@localhost:5432/test")
fancy_engine = fancy(engine)


async def create_all_tables():
    async with engine.begin() as conn:
        await conn.run_sync(tb.metadata.create_all)


async def drop_all_tables():
    async with engine.begin() as conn:
        await conn.run_sync(tb.metadata.drop_all)


async def atomic_scope():
    async with fancy_engine.atomic():
        yield


async def non_atomic_scope():
    async with fancy_engine.non_atomic():
        yield


async def transaction_dependency():
    async with engine.begin() as conn:
        yield conn


async def connection_dependency():
    async with engine.connect() as conn:
        yield conn


@asynccontextmanager
async def lifespan(app):
    print("Starting up...")
    await create_all_tables()
    yield
    print("Shutting down...")
    await drop_all_tables()
