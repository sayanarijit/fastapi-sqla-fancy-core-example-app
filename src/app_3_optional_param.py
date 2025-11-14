import random
from typing import Annotated

import sqlalchemy as sa
from fastapi import Depends, FastAPI
from sqlalchemy.ext.asyncio import AsyncConnection

import schemas
from db import connection_dependency, lifespan, transaction_dependency
from db import fancy_engine as db
from tables import Author, Book

app = FastAPI(lifespan=lifespan)
DBTransaction = Annotated[AsyncConnection | None, Depends(transaction_dependency)]
DBConnection = Annotated[AsyncConnection | None, Depends(connection_dependency)]


@app.get("/books", response_model=list[schemas.Book])
async def get_books(author_name: str | None = None, tr: DBTransaction = None):
    qry = (
        sa.select(Book.title, Author.name.label("author_name"))
        .select_from(Book.Table)
        .join(Author.Table)
    )
    if author_name:
        qry = qry.where(Author.name == author_name)
    res = await db.tx(tr, qry)
    return res.mappings().all()


@app.get("/authors", response_model=list[schemas.Author])
async def get_authors(tr: DBTransaction = None):
    qry = sa.select(Author.id, Author.name)
    res = await db.tx(tr, qry)
    return res.mappings().all()


@app.post("/books", response_model=int)
async def create_book(payload: schemas.CreateBook, tr: DBTransaction = None):
    author_id = payload.author_id
    if not author_id and payload.author_name:
        qry = (
            sa.insert(Author.Table)
            .values({Author.name: payload.author_name})
            .returning(Author.id)
        )
        res = await db.tx(tr, qry)
        author_id = res.scalar_one()

        # Simulate a random failure after creating the author to test rollback
        if random.choice([True, False]):
            raise ValueError("Simulated failure: author_id is required")

    qry = (
        sa.insert(Book.Table)
        .values({Book.title: payload.title, Book.author_id: author_id})
        .returning(Book.id)
    )
    res = await db.tx(tr, qry)
    return res.scalar_one()


@app.get("/stats")
async def get_stats(tr: DBConnection = None):
    """Perform a data integrity check between books and authors."""

    author_count = (
        await db.tx(tr, sa.select(sa.func.count("*")).select_from(Author.Table))
    ).scalar_one()
    max_author_id = (
        await db.tx(tr, sa.select(sa.func.max(Author.id)).select_from(Author.Table))
    ).scalar_one()

    book_count = (
        await db.tx(tr, sa.select(sa.func.count("*")).select_from(Book.Table))
    ).scalar_one()
    max_book_id = (
        await db.tx(tr, sa.select(sa.func.max(Book.id)).select_from(Book.Table))
    ).scalar_one()

    return {
        "book count": book_count,
        "author count": author_count,
        "max book id": max_book_id,
        "max author id": max_author_id,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app_3_optional_param:app")
