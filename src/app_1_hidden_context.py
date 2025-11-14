"""An example FastAPI app demonstrating the hidden context pattern.

### Summary

- Wrap the SQLAlchemy's `Engine/AsyncEngine` instance with `fancy(engine)` wrapper.
    >>> fancy_engine = fancy(engine)

- Use `fancy_engine`'s `ax()` or `atx()` to execute queries in `atomic()` transaction.
    >>> async with fancy_engine.atomic():  # Creates a new transaction
    ...     await fancy_engine.ax(...)  # Uses the transaction
    ...     await fancy_engine.atx(...)  # Uses the transaction

- `ax()` makes sure it's called inside an `atomic()` scope. Calling outside raises error.

- `atx()` can be called outside an `atomic()` scope. In that case, it will start a new
  transaction automatically to execute the query.
    >>> async with fancy_engine.atomic():  # Creates a new transaction
    ...     await fancy_engine.atx(...)  # Uses the transaction
    >>> await fancy_engine.atx(...)  # Creates a new transaction.

- Use `fancy_engine`'s `nax()` to execute queries in `non_atomic()` connection.
    >>> async with fancy_engine.non_atomic():  # Aquires a new connection
    ...     await fancy_engine.nax(...)  # Uses the connection

- `nax()` can also be called inside `atomic()` scope. In that case, it will use atomic
  scope's transactional connection to execute the query.
    >>> async with fancy_engine.atomic():  # Creates a new transaction
    ...     await fancy_engine.nax(...)  # Uses the transaction

- `nax()` can also be called outside the `atomic()` or `non_atomic()` scope. In that
  case, it will aquire a new connection to run the query.
    >>> async with fancy_engine.non_atomic():  # Aquires a new connection
    ...     await fancy_engine.nax(...)  # Uses the connection
    >>> await fancy_engine.nax(...)  # Aquires a new connection

### Advantages

- **Less boilerplate**: Allows you to call the handlers (`create_book()`, `get_books()`
  etc.) directly, without passing around connection/transaction, making it possible to
  re-use the same handlers in background tasks or IPython shell.
    >>> await create_book(...)  # Creates its own atomic transaction
    >>> await get_books(...)  # Creates its own non-atomic connection

- **Simple & clean**: Hides connection/transaction management from route handlers. i.e. one
  less variable to pass around.

- **Complex is possible**: Allows you to optionally start an atomic/non-atomic scope and
  call the handlers within that scope to re-use the same transaction/connection.
    >>> async with fancy_engine.atomic():  # Create an atomic transaction
    ...     await create_book(...)  # Uses the same atomic transaction
    ...     await get_book(...)  # Uses the same atomic transaction

- **Mix with other patterns**: Allows you to mix with other patterns that expect a
  connection/transaction object as function parameter.
    >>> async with fancy_engine.atomic() as tr:  # Create an atomic transaction
    ...     await create_book(...)  # Uses the same atomic transaction
    ...     await a_function_that_expects_tr(..., tr=tr)  # Uses the passed transaction

- **Ensures same transaction**: Using `ax()` makes sure that the transaction started by
  `atomic()` scope is used within that scope.

### Disadvantages

- **Hidden operations**: Hidden context management can make it harder to understand the
  lifecycle of connections/transactions.

- **Unfamiliar API**: Must use the fancy wrapper's `atomic()`/`non_atomic()` methods
  instead of the standard `begin()`/`connect()` to start transaction/connection, and use
  the `ax()`, `atx()` and `nax()` methods instead of the standard `execute()` to execute
  queries.

- **Different routers**: Need to create different routers for atomic and non-atomic
  handlers.
"""

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
