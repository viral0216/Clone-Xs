"""Config request/response models."""

from pydantic import BaseModel


class ConfigUpdateRequest(BaseModel):
    yaml_content: str
    path: str = "config/clone_config.yaml"


class ConfigDiffRequest(BaseModel):
    file_a: str
    file_b: str
