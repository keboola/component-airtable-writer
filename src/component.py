import logging
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from configuration import Configuration
from utils import (
    fetch_field_mapping,
    map_records,
    get_or_create_table,
    get_table_schema,
    compare_schemas,
    process_records_batch,
)
import pandas as pd


class Component(ComponentBase):
    def run(self):
        params = Configuration(**self.configuration.parameters)

        # Get input data first
        input_tables = self.get_input_tables_definitions()
        if not input_tables:
            raise UserException("No input tables found")
        input_table_path = input_tables[0].full_path
        df = pd.read_csv(input_table_path)

        logging.info(f"📊 Loaded input data: {len(df)} rows, {len(df.columns)} columns")
        logging.info(f"📋 Input columns: {list(df.columns)}")

        try:
            logging.info(
                f"Connecting to Airtable base '{params.base_id}', table '{params.table_name}'..."
            )

            # Get or create table based on input data
            table = get_or_create_table(
                params.api_token, params.base_id, params.table_name, df
            )

            # Get field mapping for the table
            field_mapping, computed_fields = fetch_field_mapping(table, df)

            # Compare schemas and log any differences
            table_schema = get_table_schema(table)
            schema_comparison = compare_schemas(df, table_schema)

            if schema_comparison["needs_update"]:
                logging.warning(
                    "⚠️ The following input columns are missing in Airtable",
                    f" and will not be written: {schema_comparison['missing_in_table']}",
                )

            # Process the records with enhanced batch functionality
            records = df.to_dict(orient="records")
            mapped_records = map_records(records, field_mapping)

            # TODO: Make upsert key fields configurable via params
            # Using new batch processing - always batches even for single records
            process_records_batch(table, mapped_records, upsert_key_fields=None)

        except Exception as e:
            logging.error(f"❌ Failed to process data: {e}")
            raise UserException(f"Failed to process data: {str(e)}")


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
