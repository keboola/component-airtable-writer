import logging
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from configuration import Configuration
from client_airtable import (
    build_field_mapping,
    map_records,
    get_or_create_table,
    get_table_schema,
    compare_schemas,
    process_records_batch,
    clear_table_for_full_load,
    get_primary_key_fields,
    validate_connection,
)
from pyairtable import Api
import pandas as pd


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
            validate_connection(self.api, self.params.base_id)

            table = get_or_create_table(
                self.api,
                self.params.base_id,
                self.params.table_name,
                df,
                self.params.destination.columns,
            )

            # Build field mapping for the table
            field_mapping = build_field_mapping(self.params.destination.columns)

            # Only use columns present in the mapping
            mappable_columns = [col for col in df.columns if col in field_mapping]
            filtered_df = df[mappable_columns]
            logging.info(
                f"📊 Processing {len(mappable_columns)} columns: {mappable_columns}"
            )

            # Compare schemas and log any differences
            table_schema = get_table_schema(table)
            schema_comparison = compare_schemas(df, table_schema)

            if schema_comparison["needs_update"]:
                logging.warning(
                    "⚠️ The following input columns are missing in Airtable",
                    f" and will not be written: {schema_comparison['missing_in_table']}",
                )

            # Process the records with enhanced batch functionality
            records = filtered_df.to_dict(orient="records")
            mapped_records = map_records(records, field_mapping)

            load_type = self.params.destination.load_type
            upsert_key_fields = None
            # Determine upsert key fields based on load type
            if load_type == "Incremental Load":
                upsert_key_fields = get_primary_key_fields(
                    self.params.destination.columns
                )
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


@sync_action("test_connection")
def action_testConnection(self):
    """Test Airtable API token by listing bases"""
    if not self.params.api_token:
        raise UserException("API token must be set to test the connection.")
    try:
        self.api.bases()
        return {"status": "success", "message": "Connection successful."}
    except Exception as e:
        raise UserException(f"Failed to connect to Airtable: {e}")


@sync_action("list_bases")
def action_list_bases(self):
    """List all accessible Airtable bases for dropdown."""
    if not self.params.api_token:
        raise UserException("API token must be set to list bases.")
    bases = self.api.bases()
    return [{"value": b["id"], "label": b["name"]} for b in bases]


@sync_action("list_tables")
def action_list_tables(self):
    """List all tables in the selected base for dropdown."""
    if not self.params.api_token:
        raise UserException("API token must be set to list tables.")
    if not self.params.base_id:
        raise UserException("Base ID must be set to list tables.")
    base = self.api.base(self.params.base_id)
    schema = base.schema()
    return [{"value": t["name"], "label": t["name"]} for t in schema["tables"]]


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
