import random
from typing import Annotated

import sqlalchemy as sa
from fastapi import Depends, FastAPI
from sqla_fancy_core import Inject, transact
from sqla_fancy_core.decorators import connect
from sqlalchemy.ext.asyncio import AsyncConnection

import db
import schemas
from tables import Author, Book

app = FastAPI(lifespan=db.lifespan)
DBTransaction = Annotated[AsyncConnection, Depends(db.transaction_dependency)]
DBConnection = Annotated[AsyncConnection, Depends(db.connection_dependency)]


@transact
@app.get("/books", response_model=list[schemas.Book])
async def get_books(
    author_name: str | None = None, tr: DBTransaction = Inject(db.engine)
):
    qry = (
        sa.select(Book.title, Author.name.label("author_name"))
        .select_from(Book.Table)
        .join(Author.Table)
    )
    if author_name:
        qry = qry.where(Author.name == author_name)
    res = await tr.execute(qry)
    return res.mappings().all()


@transact
@app.get("/authors", response_model=list[schemas.Author])
async def get_authors(tr: DBTransaction = Inject(db.engine)):
    qry = sa.select(Author.id, Author.name)
    res = await tr.execute(qry)
    return res.mappings().all()


@transact
@app.post("/books", response_model=int)
async def create_book(
    payload: schemas.CreateBook, tr: DBTransaction = Inject(db.engine)
):
    author_id = payload.author_id
    if not author_id and payload.author_name:
        qry = (
            sa.insert(Author.Table)
            .values({Author.name: payload.author_name})
            .returning(Author.id)
        )
        res = await tr.execute(qry)
        author_id = res.scalar_one()

        # Simulate a random failure after creating the author to test rollback
        if random.choice([True, False]):
            raise ValueError("Simulated failure: author_id is required")

    qry = (
        sa.insert(Book.Table)
        .values({Book.title: payload.title, Book.author_id: author_id})
        .returning(Book.id)
    )
    res = await tr.execute(qry)
    return res.scalar_one()


@connect
@app.get("/stats")
async def get_stats(conn: DBConnection = Inject(db.engine)):
    """Perform a data integrity check between books and authors."""

    author_count = (
        await conn.execute(sa.select(sa.func.count("*")).select_from(Author.Table))
    ).scalar_one()
    max_author_id = (
        await conn.execute(sa.select(sa.func.max(Author.id)).select_from(Author.Table))
    ).scalar_one()

    book_count = (
        await conn.execute(sa.select(sa.func.count("*")).select_from(Book.Table))
    ).scalar_one()
    max_book_id = (
        await conn.execute(sa.select(sa.func.max(Book.id)).select_from(Book.Table))
    ).scalar_one()

    return {
        "book count": book_count,
        "author count": author_count,
        "max book id": max_book_id,
        "max author id": max_author_id,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app_2_decorator:app")
