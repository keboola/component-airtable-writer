import logging
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from configuration import Configuration
from utils import validate_airtable_connection, map_records, process_records
import pandas as pd


class Component(ComponentBase):
    writer_cache: dict = {}

    def run(self):
        params = Configuration(**self.configuration.parameters)
        logging.info(
            f"Connecting to Airtable base '{params.base_id}', table '{params.table_name}'..."
        )

        table, field_mapping, computed_fields = validate_airtable_connection(
            params.api_token, params.base_id, params.table_name
        )
        # self.writer_cache[self.table_name] = AirtableWriterCacheEntry(field_mapping, computed_fields)

        input_tables = self.get_input_tables_definitions()
        if not input_tables:
            raise UserException("No input tables found")
        input_table_path = input_tables[0].full_path
        df = pd.read_csv(input_table_path)
        records = df.to_dict(orient="records")

        mapped_records = map_records(records, field_mapping, computed_fields)
        process_records(table, mapped_records)


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
