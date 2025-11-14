"""An example FastAPI app demonstrating the dependency injection pattern.

### Summary

- Add `@transact` decorator to the functions that must use a transactional connection.

- Add `@connect` decorator to the functions that must use a regular/transactional
  connection.

- Mark the parameter that should receive a `Connection/AsyncConnection` by declaring
  `Inject(engine)` as the default param.

- Use the standard `execute()` method of the passed connection to execute queries.
    >>> @transact
    ... async def create_book(..., tr=Inject(engine)):
    ...     await tr.execute(...)
    >>> @connect
    ... async def get_books(..., conn=Inject(engine)):
    ...     await conn.execute(...)

- Calling the function without passing a connection explicitly to the connection param
  will automatically start a new transaction/connection and pass it as the value.
    >>> await create_book(...)  # Starts a new transaction
    >>> await get_books(...)  # Aquires a new connection

- Calling the function with an explicit connection as the value to the marked param will
  use the passed connection.
    >>> async with engine.begin() as tr:
    ...     await create_book(..., tr=tr)  # Uses the passed transaction
    ...     await get_books(..., conn=tr)  # Uses the passed transaction

- You can pass a transactional connection to functions decorated with `@connect`, but
  passing a non transactional connection to functions decorated with `@transact` will
  raise error.

### Advantages

- **Less boilerplate**: Allows you to call the handlers (`create_book()`, `get_books()`
  etc.) directly, without passing around connection/transaction, making it possible to
  re-use the same handlers in background tasks or IPython shell.
    >>> await create_book(...)  # Creates its own atomic transaction
    >>> await get_books(...)  # Creates its own non-atomic connection

- **Explicit**: Connection/transaction can be explicitly passed as parameter.

- **Familiar API**: You can use the standard SQLAlchemy's `begin()`, `connect()` and
  `execute()` methods.

### Disadvantages

- **Increased boilerplate**: Each route handler must use the `@connect` or `@transact`
  decorators and the `Inject` marker. Also, passing around the connection/transaction
  parameter when using explicit connection/transaction context adds extra boilerplate.

- **Import time overhead**: Usage of additional decorators can cause slight increase in
  import/load time.

- **Limits enforcement of same connection**: Since `Inject(engine)` aquires a new
  connection if it's not given, (unlike `ax()`), you cannot enforce that the same
  connection object is passed to every function in a multi-step operation.
    >>> async with engine.begin() as tr:  # Create an explicit transaction
    ...     await create_book(... tr=tr)  # Uses the passed transaction
    ...     await get_book(...)  # Aquires a new connection because tr is not passed
"""

import random
from typing import Annotated

import sqlalchemy as sa
from fastapi import Depends, FastAPI
from sqla_fancy_core import Inject, connect, transact
from sqlalchemy.ext.asyncio import AsyncConnection

import db
import schemas
from tables import Author, Book

DBTransaction = Annotated[AsyncConnection, Depends(db.transaction_dependency)]
DBConnection = Annotated[AsyncConnection, Depends(db.connection_dependency)]

app = FastAPI(lifespan=db.lifespan)


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
@app.get("/books", response_model=list[schemas.Book])
async def get_books(
    author_name: str | None = None, conn: DBConnection = Inject(db.engine)
):
    qry = (
        sa.select(Book.title, Author.name.label("author_name"))
        .select_from(Book.Table)
        .join(Author.Table)
    )
    if author_name:
        qry = qry.where(Author.name == author_name)
    res = await conn.execute(qry)
    return res.mappings().all()


@connect
@app.get("/authors", response_model=list[schemas.Author])
async def get_authors(conn: DBConnection = Inject(db.engine)):
    qry = sa.select(Author.id, Author.name)
    res = await conn.execute(qry)
    return res.mappings().all()


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

    uvicorn.run("app_2_dependency_injection:app")
