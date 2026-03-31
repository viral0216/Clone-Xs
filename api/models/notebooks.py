"""Notebook request/response models."""

from pydantic import BaseModel


class NotebookCell(BaseModel):
    id: str
    type: str  # "sql" | "markdown"
    content: str


class NotebookCreate(BaseModel):
    title: str
    cells: list[NotebookCell]


class NotebookUpdate(BaseModel):
    title: str | None = None
    cells: list[NotebookCell] | None = None


class NotebookResponse(BaseModel):
    id: str
    title: str
    cells: list[NotebookCell]
    created_at: str
    updated_at: str


class NotebookListItem(BaseModel):
    id: str
    title: str
    cell_count: int
    updated_at: str
