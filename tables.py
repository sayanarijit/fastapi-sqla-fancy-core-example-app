import sqlalchemy as sa
from sqla_fancy_core import TableFactory

tf = TableFactory()


class Author:
    id = tf.auto_id()
    name = tf.string("name")
    created_at = tf.created_at()
    updated_at = tf.updated_at()

    Table = tf("author")


class Book:
    id = tf(sa.Column("id", sa.Integer, primary_key=True, autoincrement=True))
    title = tf(sa.Column("title", sa.String(255), nullable=False))
    author_id = tf(sa.Column("author_id", sa.Integer, sa.ForeignKey(Author.id)))
    created_at = tf(
        sa.Column(
            "created_at",
            sa.DateTime,
            nullable=False,
            server_default=sa.func.now(),
        )
    )
    updated_at = tf(
        sa.Column(
            "updated_at",
            sa.DateTime,
            nullable=False,
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        )
    )

    Table = tf(sa.Table("book", sa.MetaData()))
