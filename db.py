from contextlib import asynccontextmanager

from sqla_fancy_core import fancy
from sqlalchemy.ext.asyncio import create_async_engine

from tables import tf

engine = create_async_engine("sqlite+aiosqlite:///example.db")
fancy_engine = fancy(engine)
atomic = fancy_engine.atomic
run = fancy_engine.atx


async def create_all_tables():
    async with engine.begin() as conn:
        await conn.run_sync(tf.metadata.create_all)


async def drop_all_tables():
    async with engine.begin() as conn:
        await conn.run_sync(tf.metadata.drop_all)


async def transaction_middleware(request, call_next):
    async with atomic() as session:
        request.state.db = session
        response = await call_next(request)
    return response


@asynccontextmanager
async def lifespan(app):
    print("Starting up...")
    await create_all_tables()
    yield
    print("Shutting down...")
    await drop_all_tables()
