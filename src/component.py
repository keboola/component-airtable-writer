import logging

import pandas as pd
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import MessageType, SelectElement, ValidationResult
from pyairtable import Api

from client_airtable import (
    build_field_mapping,
    clear_table_for_full_load,
    compare_schemas,
    get_or_create_table,
    get_primary_key_fields,
    get_sapi_column_definition,
    get_table_schema,
    map_records,
    map_to_airtable_type,
    process_records_batch,
)
from configuration import Configuration


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self.api = Api(self.params.api_token)

    def run(self):
        # Get input data first
        input_tables = self.get_input_tables_definitions()
        if not input_tables:
            raise UserException("No input tables found")
        input_table_path = input_tables[0].full_path
        df = pd.read_csv(input_table_path)
        logging.info(f"Loaded input data: {len(df)} rows")

        try:
            table = get_or_create_table(
                self.api,
                self.params.base_id,
                self.params.destination.table_name,
                df,
                self.params.destination.columns,
            )

            # Build field mapping for the table
            field_mapping = build_field_mapping(self.params.destination.columns)

            # Only use columns present in the mapping
            mappable_columns = [col for col in df.columns if col in field_mapping]
            filtered_df = df[mappable_columns]
            logging.info(f"📊 Processing {len(mappable_columns)} columns: {mappable_columns}")

            # Compare schemas and log any differences
            table_schema = get_table_schema(table)
            schema_comparison = compare_schemas(df, table_schema)

            if schema_comparison["missing_in_table"]:
                logging.warning(
                    f"⚠️ The following input columns are missing in Airtable "
                    f"and will not be written: {schema_comparison['missing_in_table']}"
                )

            # Process the records with enhanced batch functionality
            records = filtered_df.to_dict(orient="records")
            mapped_records = map_records(records, field_mapping, self.params.destination.columns)

            load_type = self.params.destination.load_type
            upsert_key_fields = None
            # Determine upsert key fields based on load type
            if load_type == "Incremental Load":
                upsert_key_fields = get_primary_key_fields(self.params.destination.columns)
                if not upsert_key_fields:
                    raise UserException(
                        "Incremental load requires at least one primary key field to be set in the configuration."
                    )
            elif self.params.destination.load_type == "Full Load":
                clear_table_for_full_load(table)
                upsert_key_fields = None
            elif self.params.destination.load_type == "Append":
                upsert_key_fields = None

            # Using new batch processing - always batches even for single records
            process_records_batch(
                table,
                mapped_records,
                load_type=load_type,
                upsert_key_fields=upsert_key_fields,
            )

        except Exception as e:
            raise UserException(f"Failed to process data: {str(e)}")

    # --- Keboola Sync Actions ---
    @sync_action("testConnection")
    def testConnection(self):
        """Test Airtable API token by listing bases"""
        if not self.params.api_token:
            raise UserException("API token must be set to test the connection.")
        try:
            self.api.bases()
        except Exception as e:
            raise UserException(f"Failed to connect to Airtable: {e}")
        return ValidationResult(
            "Connection successful",
            MessageType.SUCCESS,
        )

    @sync_action("list_bases")
    def list_bases(self):
        """List all accessible Airtable bases for dropdown."""
        if not self.params.api_token:
            raise UserException("API token must be set to list bases.")
        return [SelectElement(b.id, b.name) for b in self.api.bases()]

    @sync_action("list_tables")
    def list_tables(self):
        """List all tables in the selected base for dropdown."""
        if not self.params.api_token:
            raise UserException("API token must be set to list tables.")
        if not self.params.base_id:
            raise UserException("Base ID must be set to list tables.")
        base = self.api.base(self.params.base_id)
        return [SelectElement(t.id, t.name) for t in base.schema().tables]

    @sync_action("return_columns_data")
    def return_columns_data(self):
        """Load columns from input mapping and return configuration data."""
        # 1. Get input table mapping (raise error if not configured)
        if not self.configuration.tables_input_mapping or len(self.configuration.tables_input_mapping) != 1:
            raise UserException(
                "Exactly one input table must be mapped in the configuration. "
                "Please add an input table mapping in the UI or configuration."
            )

        # 2. Get table definition from Keboola Storage API
        table_id = self.configuration.tables_input_mapping[0].source
        columns = get_sapi_column_definition(
            table_id,
            self.environment_variables.url,
            self.environment_variables.token,
        )

        # 3. Map Keboola data types to Airtable field types
        for col in columns:
            col["dtype"] = map_to_airtable_type(col["dtype"])

        # 4. Return configuration data
        return {
            "type": "data",
            "data": {
                "base_id": self.params.base_id,
                "destination": {
                    "table_name": self.params.destination.table_name,
                    "load_type": self.params.destination.load_type,
                    "columns": columns,
                },
            },
        }


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
