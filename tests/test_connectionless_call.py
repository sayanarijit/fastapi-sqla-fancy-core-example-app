import os
import sys
from pathlib import Path

import pytest
import pytest_asyncio

# Add parent directory to path to import app modules
sys.path.insert(0, os.path.join(str(Path(__file__).parent.parent), "src"))

import app_1_hidden_context as app1
import app_2_decorator as app2
import app_3_optional_param as app3
import db


@pytest_asyncio.fixture(scope="function", autouse=True)
async def setup_database():
    """Setup and teardown database for each test."""
    await db.create_all_tables()
    yield
    await db.drop_all_tables()


@pytest.mark.asyncio
async def test_functions_can_be_called_without_explicit_transaction_or_connection():
    assert (
        (await app1.get_stats())
        == (await app2.get_stats())
        == (await app3.get_stats())
        == {
            "book count": 0,
            "author count": 0,
            "max book id": None,
            "max author id": None,
        }
    )
