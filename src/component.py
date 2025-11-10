import logging
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import MessageType, SelectElement, ValidationResult
import fireducks.pandas as pd
from client_airtable import AirtableClient
from configuration import Configuration
from utils import get_sapi_column_definition, map_to_airtable_type

# FireDucks has very verbose logging, if debug flag = true.
logging.getLogger("fireducks").setLevel(logging.INFO)
logging.getLogger("firefw").setLevel(logging.INFO)


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self.airtable_client = AirtableClient(self.params)

    def run(self):
        # Get input data first
        input_tables = self.get_input_tables_definitions()
        if not input_tables:
            raise UserException("No input tables found")
        input_table_path = input_tables[0].full_path
        df = pd.read_csv(input_table_path)

        try:
            # Get or create table using the client
            table = self.airtable_client.get_or_create_table(df)

            # Build field mapping for the table
            field_mapping = self.airtable_client.build_field_mapping()

            # Only use columns present in the mapping
            mappable_columns = [col for col in df.columns if col in field_mapping]
            filtered_df = df[mappable_columns]
            logging.info(f"📊 Processing {len(mappable_columns)} columns: {mappable_columns}")

            # Compare schemas and log any differences
            self.airtable_client.compare_schemas(df, self.airtable_client.get_table_schema(table))

            # Process the records with enhanced batch functionality
            records = filtered_df.to_dict(orient="records")
            mapped_records = self.airtable_client.map_records(records, field_mapping)

            load_type = self.params.destination.load_type
            # Handle Full Load mode
            if load_type == "Full Load":
                self.airtable_client.clear_table_for_full_load(table)
            # Validate Incremental Load has primary keys
            elif load_type == "Incremental Load":
                upsert_key_fields = self.airtable_client.get_primary_key_fields()
                if not upsert_key_fields:
                    raise UserException(
                        "Incremental load requires at least one primary key field to be set in the configuration."
                    )

            # Process records using the client
            self.airtable_client.process_records_batch(table, mapped_records, load_type)

        except Exception as e:
            raise UserException(f"Failed to process data: {str(e)}")

    # --- Keboola Sync Actions ---
    @sync_action("testConnection")
    def testConnection(self):
        """Test Airtable API token by listing bases"""
        if not self.params.api_token:
            raise UserException("API token must be set to test the connection.")
        try:
            self.airtable_client.test_connection()
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
        return [SelectElement(b.id, b.name) for b in self.airtable_client.list_bases()]

    @sync_action("list_tables")
    def list_tables(self):
        """List all tables in the selected base for dropdown."""
        if not self.params.api_token:
            raise UserException("API token must be set to list tables.")
        if not self.params.base_id:
            raise UserException("Base ID must be set to list tables.")
        return [SelectElement(t.id, t.name) for t in self.airtable_client.list_tables()]

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
