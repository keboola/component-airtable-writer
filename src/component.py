import csv
import logging
from dataclasses import dataclass
from datetime import datetime
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from configuration import Configuration
from src.utils import (
    validate_airtable_connection,
    map_records,
    process_records,
    write_job_log,
)


@dataclass
class AirtableWriterCacheEntry:
    field_mapping: dict
    computed_fields: list


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self.base_id = self.params.base_id if hasattr(self.params, 'base_id') else self.params.get('base_id')
        self.table_name = self.params.table_name if hasattr(self.params, 'table_name') else self.params.get('table_name')
        self.api_token = getattr(self.params, 'personal_access_token', None) or self.params.get('#personal_access_token')
        self.writer_cache = {}

    def run(self):
        import pandas as pd
        import os

        if not all([self.base_id, self.table_name, self.api_token]):
            raise UserException("Missing one or more required parameters: base_id, table_name, #personal_access_token")

        logging.info(f"Connecting to Airtable base '{self.base_id}', table '{self.table_name}'...")
        table, field_mapping, computed_fields = validate_airtable_connection(self.api_token, self.base_id, self.table_name)
        self.writer_cache[self.table_name] = AirtableWriterCacheEntry(field_mapping, computed_fields)

        input_tables = self.get_input_tables_definitions()
        if not input_tables:
            raise UserException("No input tables found")
        input_table_path = input_tables[0].full_path
        df = pd.read_csv(input_table_path)
        records = df.to_dict(orient="records")

        mapped_records = map_records(records, field_mapping, computed_fields)
        log_rows = process_records(table, mapped_records)
        write_job_log(self, log_rows)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
