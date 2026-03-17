"""Config request/response models."""

from pydantic import BaseModel


class ConfigUpdateRequest(BaseModel):
    yaml_content: str
    path: str = "config/clone_config.yaml"


class ConfigDiffRequest(BaseModel):
    config_a: dict | str
    config_b: dict | str
