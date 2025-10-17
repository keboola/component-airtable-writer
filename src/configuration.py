from enum import Enum
from typing import Optional
from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, model_validator


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
    table: Optional[str] = None
    columns: list[ColumnConfig] = Field(default_factory=list)
    load_type: LoadType = Field(default=LoadType.append)


class Configuration(BaseModel):
    base_id: str = Field(alias="base_id")
    table_name: str = Field(alias="table_name")
    api_token: str = Field(alias="#api_token")
    destination: Destination = Field(default_factory=Destination)
    debug: bool = False

    @model_validator(mode="after")
    def check_token(self):
        if not self.api_token:
            raise UserException("API token must be entered")
        return self

    @model_validator(mode="after")
    def check_table_name(self):
        if not self.table_name:
            raise UserException("Table Name must be entered")
        return self

    @model_validator(mode="after")
    def check_base_id(self):
        if not self.base_id:
            raise UserException("Base ID must be entered")
        return self
