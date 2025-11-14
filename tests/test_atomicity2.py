import asyncio
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
import sqlalchemy as sa

# Add parent directory to path to import app modules
sys.path.insert(0, os.path.join(str(Path(__file__).parent.parent), "src"))

import db
from app_2_dependency_injection import app
from tables import Author, Book


@pytest_asyncio.fixture(scope="function", autouse=True)
async def setup_database():
    """Setup and teardown database for each test."""
    await db.create_all_tables()
    yield
    await db.drop_all_tables()


@pytest_asyncio.fixture
async def async_client():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def get_counts():
    """Get the current count of books and authors."""
    books_qry = sa.select(sa.func.count()).select_from(Book.Table)
    authors_qry = sa.select(sa.func.count()).select_from(Author.Table)

    books_count = (await db.fancy_engine.nax(books_qry)).scalar_one()
    authors_count = (await db.fancy_engine.nax(authors_qry)).scalar_one()

    return books_count, authors_count


async def create_book_async(client: httpx.AsyncClient, book_number: int):
    """Make a single book creation request."""
    try:
        response = await client.post(
            "/books",
            json={
                "title": f"Book {book_number}",
                "author_name": f"Author {book_number}",
            },
        )
        return response.status_code == 200
    except Exception:
        # Catch any exceptions (including those during cleanup)
        return False


@pytest.mark.asyncio
async def test_concurrent_async_atomicity(async_client):
    """
    Test 1: Concurrent async requests using asyncio.gather

    Due to the random failure in create_book endpoint, approximately 50% of
    requests will fail and rollback. The test verifies that books count equals
    authors count, proving that transactions are atomic.
    """
    # Get initial counts
    initial_books, initial_authors = await get_counts()
    assert initial_books == 0 and initial_authors == 0

    # Make 500 concurrent requests
    num_requests = 500
    tasks = [create_book_async(async_client, i) for i in range(num_requests)]
    results = await asyncio.gather(*tasks)

    # Count successful requests
    successful_requests = sum(results)

    # Get final counts
    final_books, final_authors = await get_counts()

    # Calculate actual new records
    new_books = final_books - initial_books
    new_authors = final_authors - initial_authors

    assert new_books > 0, "No new books were created"

    # Assert atomicity: books count must equal authors count
    assert new_books == new_authors, (
        f"Atomicity violated: {new_books} books but {new_authors} authors"
    )

    # Assert that counts match successful requests
    assert new_books == successful_requests, (
        f"Expected {successful_requests} new books but found {new_books}"
    )
    assert new_authors == successful_requests, (
        f"Expected {successful_requests} new authors but found {new_authors}"
    )

    print(
        f"\n✓ Concurrent async test: {successful_requests}/{num_requests} requests succeeded"
    )
    print(f"  Books: {new_books}, Authors: {new_authors}")


@pytest.mark.asyncio
async def test_multithreading_atomicity(async_client):
    initial_books, initial_authors = await get_counts()
    assert initial_books == 0 and initial_authors == 0
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [
            executor.submit(asyncio.run, create_book_async(async_client, i))
            for i in range(20)
        ]
        [future.result() for future in futures]

    final_books, final_authors = await get_counts()
    new_books = final_books - initial_books
    new_authors = final_authors - initial_authors

    assert new_books > 0, "No new books were created"

    assert new_books == new_authors, (
        f"Atomicity violated: {new_books} books but {new_authors} authors"
    )
    print(f"  Books: {new_books}, Authors: {new_authors}")
