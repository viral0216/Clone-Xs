"""Notebooks endpoints — CRUD for SQL Notebooks in Data Lab."""

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException

from api.models.notebooks import (
    NotebookCreate,
    NotebookListItem,
    NotebookResponse,
    NotebookUpdate,
)

router = APIRouter()

# Store notebooks as JSON files
NOTEBOOKS_DIR = Path(__file__).resolve().parent.parent.parent / "notebooks" / "sql_notebooks"
NOTEBOOKS_DIR.mkdir(parents=True, exist_ok=True)


def _load_notebook(notebook_id: str) -> dict:
    path = NOTEBOOKS_DIR / f"{notebook_id}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Notebook {notebook_id} not found")
    return json.loads(path.read_text())


def _save_notebook(data: dict):
    path = NOTEBOOKS_DIR / f"{data['id']}.json"
    path.write_text(json.dumps(data, indent=2))


@router.get("", response_model=list[NotebookListItem], summary="List all notebooks")
async def list_notebooks():
    """List all saved notebooks with basic metadata."""
    notebooks = []
    for f in sorted(NOTEBOOKS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            data = json.loads(f.read_text())
            notebooks.append(NotebookListItem(
                id=data["id"],
                title=data.get("title", "Untitled"),
                cell_count=len(data.get("cells", [])),
                updated_at=data.get("updated_at", ""),
            ))
        except Exception:
            continue
    return notebooks


@router.get("/{notebook_id}", response_model=NotebookResponse, summary="Get a notebook")
async def get_notebook(notebook_id: str):
    """Get a single notebook by ID."""
    data = _load_notebook(notebook_id)
    return NotebookResponse(**data)


@router.post("", response_model=NotebookResponse, summary="Create a notebook")
async def create_notebook(req: NotebookCreate):
    """Create a new notebook."""
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    data = {
        "id": str(uuid.uuid4())[:8],
        "title": req.title,
        "cells": [c.model_dump() for c in req.cells],
        "created_at": now,
        "updated_at": now,
    }
    _save_notebook(data)
    return NotebookResponse(**data)


@router.put("/{notebook_id}", response_model=NotebookResponse, summary="Update a notebook")
async def update_notebook(notebook_id: str, req: NotebookUpdate):
    """Update an existing notebook."""
    data = _load_notebook(notebook_id)
    if req.title is not None:
        data["title"] = req.title
    if req.cells is not None:
        data["cells"] = [c.model_dump() for c in req.cells]
    data["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    _save_notebook(data)
    return NotebookResponse(**data)


@router.delete("/{notebook_id}", summary="Delete a notebook")
async def delete_notebook_endpoint(notebook_id: str):
    """Delete a notebook."""
    path = NOTEBOOKS_DIR / f"{notebook_id}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Notebook {notebook_id} not found")
    path.unlink()
    return {"status": "deleted", "id": notebook_id}


@router.post("/{notebook_id}/export", summary="Export notebook as SQL")
async def export_notebook(notebook_id: str):
    """Export a notebook as a concatenated .sql file."""
    data = _load_notebook(notebook_id)
    lines = []
    for cell in data.get("cells", []):
        if cell["type"] == "markdown":
            for line in cell["content"].split("\n"):
                lines.append(f"-- {line}")
        else:
            lines.append(cell["content"])
        lines.append("")
    return {"filename": f"{data['title'].replace(' ', '_')}.sql", "content": "\n".join(lines)}
