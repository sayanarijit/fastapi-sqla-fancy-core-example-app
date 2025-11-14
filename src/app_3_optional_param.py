"""An example FastAPI app demonstrating the optional param pattern.

### Summary

- Wrap the SQLAlchemy's `Engine/AsyncEngine` instance with `fancy(engine)` wrapper.
    >>> fancy_engine = fancy(engine)

- Declare an optional parameter to receive the `Connection/AsyncConnection` object.

- Use `fancy_engine`'s `tx` with the optional connection to execute queries in
  transaction.
    >>> async def create_book(..., tr=None):
    ...     await fancy_engine.tx(tr, ...)

- Use `fancy_engine`'s `x` with the optional connection to execute queries with or
  without transaction.
    >>> async def get_books(..., conn=None):
    ...     await fancy_engine.x(conn, ...)

- If a connection is not passed, (i.e. conn/tr is None), a new connection/transaction is
  automatically created to execute the query.

- If a connection is passed, it will use that connection.

- If a non-transactional connection is passed to `tx()`, it will raise an error.

- If a transactional connection is passed to `x()`, it will use that transaction to
  execute the query.

- You can also use the `fancy_engine`'s `atomic()` or `non_atomic()` context to avoid
  explicitly passing the connection objects.
    >>> async with fancy_engine.atomic():  # Creates a new transaction
    ...     await create_book()  # Uses the transaction
    ...     await get_books()  # Uses the transaction

### Advantages

- **Less boilerplate**: Allows you to call the handlers (`create_book()`, `get_books()`
  etc.) directly, without passing around connection/transaction, making it possible to
  re-use the same handlers in background tasks or IPython shell.
    >>> await create_book(...)  # Creates its own atomic transaction
    >>> await get_books(...)  # Creates its own non-atomic connection

- **Explicit**: Connection/transaction can be explicitly passed as parameter.

- **Complex is possible**: Allows you to optionally start a atomic/non-atomic scope and
  call the handlers within that scope to re-use the same transaction/connection.
    >>> async with fancy_engine.atomic():  # Create an atomic transaction
    ...     await create_book(...)  # Uses the same atomic transaction
    ...     await get_book(...)  # Uses the same atomic transaction

- **Full control**: Also allows you to create your own explict transaction/connection
  and call the handlers within that scope to re-use the same transaction/connection.
    >>> async with engine.begin() as tr:  # Create an explicit transaction
    ...     await create_book(... tr=tr)  # Uses the passed transaction
    ...     await get_book(... conn=tr)  # Uses the passed transaction

### Disadvantages

- **Limits enforcement of same connection**: Since `x()` and `tx()` aquires a new
  connection if it's not given, (unlike `ax()`), you cannot enforce that the same
  connection object is passed to every function in a multi-step operation.
    >>> async with engine.begin() as tr:  # Create an explicit transaction
    ...     await create_book(... tr=tr)  # Uses the passed transaction
    ...     await get_book(...)  # Aquires a new connection because tr is not passed

- **Unfamiliar API**: Must use the fancy wrapper's `x()` and `tx()` methods instead of
  the standard `execute()` to execute queries, and optionally use the
  `atomic()`/`non_atomic()` methods instead of the standard `begin()`/`connect()` to
  start transaction/connection.
"""

import random
from typing import Annotated

import sqlalchemy as sa
from fastapi import Depends, FastAPI
from sqlalchemy.ext.asyncio import AsyncConnection

import db
import schemas
from tables import Author, Book

DBTransaction = Annotated[AsyncConnection | None, Depends(db.transaction_dependency)]
DBConnection = Annotated[AsyncConnection | None, Depends(db.connection_dependency)]

app = FastAPI(lifespan=db.lifespan)


@app.post("/books", response_model=int)
async def create_book(payload: schemas.CreateBook, tr: DBTransaction = None):
    author_id = payload.author_id
    if not author_id and payload.author_name:
        qry = (
            sa.insert(Author.Table)
            .values({Author.name: payload.author_name})
            .returning(Author.id)
        )
        res = await db.fancy_engine.tx(tr, qry)
        author_id = res.scalar_one()

        # Simulate a random failure after creating the author to test rollback
        if random.choice([True, False]):
            raise ValueError("Simulated failure: author_id is required")

    qry = (
        sa.insert(Book.Table)
        .values({Book.title: payload.title, Book.author_id: author_id})
        .returning(Book.id)
    )
    res = await db.fancy_engine.tx(tr, qry)
    return res.scalar_one()


@app.get("/books", response_model=list[schemas.Book])
async def get_books(author_name: str | None = None, conn: DBConnection = None):
    qry = (
        sa.select(Book.title, Author.name.label("author_name"))
        .select_from(Book.Table)
        .join(Author.Table)
    )
    if author_name:
        qry = qry.where(Author.name == author_name)
    res = await db.fancy_engine.x(conn, qry)
    return res.mappings().all()


@app.get("/authors", response_model=list[schemas.Author])
async def get_authors(conn: DBConnection = None):
    qry = sa.select(Author.id, Author.name)
    res = await db.fancy_engine.x(conn, qry)
    return res.mappings().all()


@app.get("/stats")
async def get_stats(conn: DBConnection = None):
    """Perform a data integrity check between books and authors."""

    author_count = (
        await db.fancy_engine.x(
            conn, sa.select(sa.func.count("*")).select_from(Author.Table)
        )
    ).scalar_one()
    max_author_id = (
        await db.fancy_engine.x(
            conn, sa.select(sa.func.max(Author.id)).select_from(Author.Table)
        )
    ).scalar_one()

    book_count = (
        await db.fancy_engine.x(
            conn, sa.select(sa.func.count("*")).select_from(Book.Table)
        )
    ).scalar_one()
    max_book_id = (
        await db.fancy_engine.x(
            conn, sa.select(sa.func.max(Book.id)).select_from(Book.Table)
        )
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
