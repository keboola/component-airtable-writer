from enum import Enum
from typing import Optional

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, model_validator


class LoadType(str, Enum):
    full_load = "Full Load"
    incremental_load = "Incremental Load"
    append = "Append"


class ColumnConfig(BaseModel):
    source_name: str
    destination_name: str
    dtype: str
    pk: bool


class Destination(BaseModel):
    table_name: Optional[str] = Field(alias="table_name", default="")
    columns: list[ColumnConfig] = Field(default_factory=list)
    load_type: LoadType = Field(default=LoadType.append)


class Configuration(BaseModel):
    base_id: str = Field(alias="base_id", default="")
    api_token: str = Field(alias="#api_token")
    destination: Destination = Field(default_factory=Destination)
    debug: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

    @model_validator(mode="after")
    def check_token(self):
        if not self.api_token:
            raise UserException("API token must be entered")
        return self
