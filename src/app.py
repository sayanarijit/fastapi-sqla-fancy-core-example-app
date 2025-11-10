import random

import sqlalchemy as sa
from fastapi import FastAPI

import db
import schemas
from tables import Author, Book

app = FastAPI(lifespan=db.lifespan)
app.middleware("http")(db.transaction_middleware)


@app.get("/books", response_model=list[schemas.Book])
async def get_books(author_name: str | None = None):
    qry = (
        sa.select(Book.title, Author.name.label("author_name"))
        .select_from(Book.Table)
        .join(Author.Table)
    )
    if author_name:
        qry = qry.where(Author.name == author_name)
    res = await db.run(qry)
    return res.mappings().all()


@app.get("/authors", response_model=list[schemas.Author])
async def get_authors():
    qry = sa.select(Author.id, Author.name)
    res = await db.run(qry)
    return res.mappings().all()


@app.post("/books", response_model=int)
async def create_book(payload: schemas.CreateBook) -> int:
    author_id = payload.author_id
    if not author_id and payload.author_name:
        qry = (
            sa.insert(Author.Table)
            .values({Author.name: payload.author_name})
            .returning(Author.id)
        )
        res = await db.run(qry)
        author_id = res.scalar_one()

        # Simulate a random failure after creating the author to test rollback
        if random.choice([True, False]):
            raise ValueError("Simulated failure: author_id is required")

    qry = (
        sa.insert(Book.Table)
        .values({Book.title: payload.title, Book.author_id: author_id})
        .returning(Book.id)
    )
    res = await db.run(qry)
    return res.scalar_one()


@app.get("/stats")
async def get_stats():
    """Perform a data integrity check between books and authors."""

    author_count = (
        await db.run(sa.select(sa.func.count("*")).select_from(Author.Table))
    ).scalar_one()
    max_author_id = (
        await db.run(sa.select(sa.func.max(Author.id)).select_from(Author.Table))
    ).scalar_one()

    book_count = (
        await db.run(sa.select(sa.func.count("*")).select_from(Book.Table))
    ).scalar_one()
    max_book_id = (
        await db.run(sa.select(sa.func.max(Book.id)).select_from(Book.Table))
    ).scalar_one()

    return {
        "book count": book_count,
        "author count": author_count,
        "max book id": max_book_id,
        "max author id": max_author_id,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="localhost", port=8000, reload=True, workers=4)
