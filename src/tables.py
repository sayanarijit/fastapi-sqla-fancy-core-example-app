import sqlalchemy as sa
from sqla_fancy_core import TableBuilder

tb = TableBuilder()


class Author:
    id = tb.auto_id()
    name = tb.string("name")
    created_at = tb.created_at()
    updated_at = tb.updated_at()

    Table = tb("author")


class Book:
    id = tb(sa.Column("id", sa.Integer, primary_key=True, autoincrement=True))
    title = tb(sa.Column("title", sa.String(255), nullable=False))
    author_id = tb(sa.Column("author_id", sa.Integer, sa.ForeignKey(Author.id)))
    created_at = tb(
        sa.Column(
            "created_at",
            sa.DateTime,
            nullable=False,
            server_default=sa.func.now(),
        )
    )
    updated_at = tb(
        sa.Column(
            "updated_at",
            sa.DateTime,
            nullable=False,
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        )
    )

    Table = tb("book")
