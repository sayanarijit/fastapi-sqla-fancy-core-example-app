from typing import Optional

from pydantic import BaseModel, Field


class Book(BaseModel):
    title: str = Field(..., description="The title of the book")
    author_name: str = Field(..., description="The name of the author")


class CreateBook(BaseModel):
    title: str = Field(..., description="The title of the book")
    author_id: Optional[int] = Field(None, description="The ID of the author")
    author_name: Optional[str] = Field(
        None, description="The name of the author (used if author_id is not provided)"
    )


class Author(BaseModel):
    id: int = Field(..., description="The ID of the author")
    name: str = Field(..., description="The name of the author")


class CreateAuthor(BaseModel):
    name: str = Field(..., description="The name of the author")
