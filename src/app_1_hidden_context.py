import random

import sqlalchemy as sa
from fastapi import APIRouter, Depends, FastAPI

import db
import schemas
from tables import Author, Book

atomic_router = APIRouter(dependencies=[Depends(db.atomic_scope)])
non_atomic_router = APIRouter(dependencies=[Depends(db.non_atomic_scope)])


@atomic_router.post("/books", response_model=int)
async def create_book(payload: schemas.CreateBook) -> int:
    author_id = payload.author_id
    if not author_id and payload.author_name:
        qry = (
            sa.insert(Author.Table)
            .values({Author.name: payload.author_name})
            .returning(Author.id)
        )
        res = await db.fancy_engine.atx(qry)
        author_id = res.scalar_one()

        # Simulate a random failure after creating the author to test rollback
        if random.choice([True, False]):
            raise ValueError("Simulated failure: author_id is required")

    qry = (
        sa.insert(Book.Table)
        .values({Book.title: payload.title, Book.author_id: author_id})
        .returning(Book.id)
    )
    res = await db.fancy_engine.atx(qry)
    return res.scalar_one()


@non_atomic_router.get("/books", response_model=list[schemas.Book])
async def get_books(author_name: str | None = None):
    qry = (
        sa.select(Book.title, Author.name.label("author_name"))
        .select_from(Book.Table)
        .join(Author.Table)
    )
    if author_name:
        qry = qry.where(Author.name == author_name)
    res = await db.fancy_engine.nax(qry)
    return res.mappings().all()


@non_atomic_router.get("/authors", response_model=list[schemas.Author])
async def get_authors():
    qry = sa.select(Author.id, Author.name)
    res = await db.fancy_engine.nax(qry)
    return res.mappings().all()


@non_atomic_router.get("/stats")
async def get_stats():
    """Perform a data integrity check between books and authors."""

    author_count = (
        await db.fancy_engine.nax(
            sa.select(sa.func.count("*")).select_from(Author.Table)
        )
    ).scalar_one()
    max_author_id = (
        await db.fancy_engine.nax(
            sa.select(sa.func.max(Author.id)).select_from(Author.Table)
        )
    ).scalar_one()

    book_count = (
        await db.fancy_engine.nax(sa.select(sa.func.count("*")).select_from(Book.Table))
    ).scalar_one()
    max_book_id = (
        await db.fancy_engine.nax(
            sa.select(sa.func.max(Book.id)).select_from(Book.Table)
        )
    ).scalar_one()

    return {
        "book count": book_count,
        "author count": author_count,
        "max book id": max_book_id,
        "max author id": max_author_id,
    }


app = FastAPI(lifespan=db.lifespan)
app.include_router(atomic_router)
app.include_router(non_atomic_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app_1_hidden_context:app")
