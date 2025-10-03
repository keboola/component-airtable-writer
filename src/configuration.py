from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, model_validator


class Configuration(BaseModel):
    base_id: str = Field(alias="base_id")
    table_name: str = Field(alias="table_name")
    api_token: str = Field(alias="#api_token")
    # debug: bool = False

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
