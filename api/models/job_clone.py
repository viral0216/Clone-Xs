"""Pydantic models for Job cloning API endpoints."""

from pydantic import BaseModel
from typing import Optional


class JobCloneRequest(BaseModel):
    job_id: int
    new_name: str = ""
    overrides: dict = {}


class CrossWorkspaceJobCloneRequest(BaseModel):
    job_id: int
    dest_host: str
    dest_token: str
    new_name: str = ""


class JobDiffRequest(BaseModel):
    job_id_a: int
    job_id_b: int


class JobBackupRequest(BaseModel):
    job_ids: list[int]


class JobRestoreRequest(BaseModel):
    definitions: list[dict]
