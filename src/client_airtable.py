import logging
import math
from datetime import datetime
from typing import Any, Iterable
from keboola.component.exceptions import UserException
from pyairtable import Api, Base, Table


class AirtableClient:
    """
    Client for interacting with Airtable API.
    Encapsulates all Airtable operations including table management, schema operations, and data processing.
    """

    def __init__(self, params):
        """
        Initialize the Airtable client.

        Args:
            api: Airtable API instance
            params: Configuration parameters containing base_id, destination settings, etc.
        """
        self.api = Api(params.api_token)
        self.params = params

    def test_connection(self) -> None:
        """Test Airtable API connection by listing bases."""
        self.api.bases()

    def list_bases(self) -> list:
        """List all accessible Airtable bases."""
        return list(self.api.bases())

    def list_tables(self) -> list:
        """List all tables in the configured base."""
        base = self.api.base(self.params.base_id)
        return list(base.schema().tables)

    def get_or_create_table(self, input_columns: list) -> Table:
        """
        Get an existing table by name, or create it if it doesn't exist.

        Args:
            input_columns: List of input column names

        Returns:
            Table object
        """
        base = self.api.base(self.params.base_id)
        table_name = self.params.destination.table_name
        column_configs = self.params.destination.columns

        try:
            base_schema = base.schema()
            existing_table_names = [table.name for table in base_schema.tables]

            if table_name in existing_table_names:
                logging.info(f"📋 Found existing table '{table_name}'")
                return base.table(table_name)
            else:
                logging.info(f"📋 Table '{table_name}' not found in base. Will create new table.")
                return self._create_table_from_config(base, table_name, column_configs)

        except Exception as e:
            logging.error(f"❌ Failed to check base schema or create table: {e}")
            raise UserException(f"Failed to access or create table '{table_name}': {e}")

    def build_field_mapping(self) -> dict[str, dict]:
        """
        Build field mapping from column configurations.

        Returns:
            Dict mapping source names to dict with destination_name and dtype
            Example: {"Score": {"destination_name": "Score", "dtype": "singleLineText", "upsert_key": False}}
        """
        column_configs = self.params.destination.columns
        if not column_configs:
            raise UserException("Column configuration is required.")
        return {
            col.source_name: {
                "destination_name": col.destination_name,
                "dtype": col.dtype,
                "upsert_key": col.upsert_key,
            }
            for col in column_configs
            if col.destination_name
        }

    def get_table_schema(self, table: Table) -> dict:
        """
        Get the current schema/configuration of an existing table.

        Args:
            table: Airtable table object

        Returns:
            Dict with schema information
        """
        try:
            table_schema = table.schema()

            schema = {
                "table_name": table_schema.name,
                "fields": [field.name for field in table_schema.fields],
                "field_types": {field.name: field.type for field in table_schema.fields},
                "primary_field_id": table_schema.primary_field_id,
            }

            logging.info(f"📋 Retrieved schema for table '{table_schema.name}': {schema['fields']}")
            return schema

        except Exception as e:
            logging.error(f"❌ Failed to get table schema: {e}")
            raise

    def compare_schemas(self, input_columns: Iterable[str], table_schema: dict) -> set:
        """
        Compare input columns with existing table schema and log any missing columns.

        Args:
            input_columns: List or iterable of input column names
            table_schema: Table schema dict

        Returns:
            Set of column names that exist in both the input and the Airtable table
        """
        input_columns = set(input_columns) - {"recordId"}
        table_fields = set(table_schema.get("fields", []))
        missing_in_table = sorted(input_columns - table_fields)

        # Calculate the overlap (columns that exist in both)
        overlap = input_columns & table_fields

        # Log warning if columns are missing in Airtable
        if missing_in_table:
            logging.warning(
                f"⚠️ The following input columns are missing in Airtable and will not be written: {missing_in_table}"
            )

        return overlap

    def map_records(self, records: list, field_mapping: dict) -> list:
        """
        Map input records to Airtable field names and convert values to match target field types.

        Args:
            records: List of input records (dicts) with only mappable columns
            field_mapping: Mapping from source names to dict with destination_name, dtype, and upsert_key

        Returns:
            List of mapped record dicts
        """
        mapped_records = []

        for rec in records:
            mapped = {}

            for source_col, value in rec.items():
                field_info = field_mapping[source_col]
                dest_field = field_info["destination_name"]
                target_dtype = field_info["dtype"]

                # Handle None/NaN/empty strings
                if self._is_null(value):
                    mapped[dest_field] = None
                # Text fields: always convert to string
                elif target_dtype in ["singleLineText", "multilineText", "email", "url", "phoneNumber"]:
                    mapped[dest_field] = str(value)
                else:
                    mapped[dest_field] = value

            mapped_records.append(mapped)

        return mapped_records

    @staticmethod
    def _is_null(value: Any) -> bool:
        """
        Check if a value should be treated as null/None.

        Args:
            value: Value to check

        Returns:
            True if value is null/None/NaN/empty string
        """
        if value is None:
            return True
        if isinstance(value, float) and math.isnan(value):
            return True
        if isinstance(value, str) and value.strip() == "":
            return True
        return False

    def process_records_batch(self, table: Table, mapped_records: list, load_type: str) -> None:
        """
        Process records using Airtable's batch API, branching by load_type.

        Args:
            table: Airtable table object
            mapped_records: List of mapped record dicts ready for Airtable
            load_type: One of 'Full Load', 'Incremental Load', 'Append'
        """
        log_rows = []
        total_processed = 0
        total_created = 0
        total_updated = 0
        total_errors = 0
        BATCH_SIZE = 10

        if load_type == "Full Load":
            # Table should already be cleared before this is called
            logging.info(f"Processing {len(mapped_records)} records as creates (full load)")
            create_results = self._process_create_batches(table, mapped_records, BATCH_SIZE)
            log_rows.extend(create_results["log_rows"])
            total_created += create_results["created_count"]
            total_errors += create_results["error_count"]
            total_processed += len(mapped_records)

        elif load_type == "Incremental Load":
            upsert_key_fields = self.get_upsert_key_fields()
            if not upsert_key_fields:
                raise UserException(
                    "Incremental load requires at least one upsert key field to be set in the configuration."
                )
            logging.info(
                f"Processing {len(mapped_records)} records as incremental upsert using keys: {upsert_key_fields}"
            )
            upsert_results = self._process_upsert_batches(table, mapped_records, upsert_key_fields, BATCH_SIZE)
            log_rows.extend(upsert_results["log_rows"])
            total_created += upsert_results["created_count"]
            total_updated += upsert_results.get("updated_count", 0)
            total_errors += upsert_results["error_count"]
            total_processed += len(mapped_records)

        elif load_type == "Append":
            logging.info(f"Processing {len(mapped_records)} records as creates (Append mode)")
            create_results = self._process_create_batches(table, mapped_records, BATCH_SIZE)
            log_rows.extend(create_results["log_rows"])
            total_created += create_results["created_count"]
            total_errors += create_results["error_count"]
            total_processed += len(mapped_records)

        else:
            raise UserException(f"Unknown load_type: {load_type}")

        # Log final summary
        logging.info(
            f"📈 Batch processing complete: {total_created} created, "
            f"{total_updated} updated, {total_errors} errors from {total_processed} total records"
        )

    def clear_table_for_full_load(self, table: Table) -> None:
        """
        Clear all records from a table for Full Load mode.

        Args:
            table: Airtable table object
        """
        logging.info("🗑️ Clearing table for Full Load...")
        try:
            all_records = table.all()
            if all_records:
                record_ids = [rec["id"] for rec in all_records]
                batch_size = 10
                for i in range(0, len(record_ids), batch_size):
                    batch = record_ids[i : i + batch_size]
                    table.batch_delete(batch)
                logging.info(f"🗑️ Deleted {len(record_ids)} existing records")
            else:
                logging.info("Table is already empty")
        except Exception as e:
            logging.error(f"❌ Failed to clear table: {e}")
            raise

    def get_upsert_key_fields(self) -> list:
        """
        Get the list of upsert key field names from column configurations.

        Returns:
            List of upsert key field names
        """
        column_configs = self.params.destination.columns
        if not column_configs:
            return []
        return [col.destination_name for col in column_configs if col.upsert_key]

    def _process_create_batches(self, table: Table, records: list, batch_size: int) -> dict:
        """
        Process create operations in batches using Airtable's batch create API.

        Args:
            table: Airtable table object
            records: List of record dicts to create
            batch_size: Maximum records per batch
        Returns:
            Dict with log_rows, created_count, error_count
        """
        log_rows = []
        created_count = 0
        error_count = 0

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]

            try:
                created_records = table.batch_create(batch)

                for record in created_records:
                    record_id = record["id"]
                    created_count += 1
                    _append_log_row(log_rows, record_id, "create")
                logging.debug(f"✅ Created batch of {len(batch)} records")

            except Exception as e:
                error_str = str(e)

                # Check for computed field error
                if "INVALID_VALUE_FOR_COLUMN" in error_str and "field is computed" in error_str:
                    field_name = "unknown"
                    if 'Field "' in error_str:
                        start = error_str.index('Field "') + len('Field "')
                        end = error_str.index('"', start)
                        field_name = error_str[start:end]

                    raise UserException(
                        f"You cannot write to a computed field {field_name}. "
                        "Computed fields are automatically calculated by Airtable and cannot accept values. "
                        "Please remove this field from your column mapping or exclude it from the input data."
                    )

                logging.error("Batch failed to insert records. Use debug for more info.")
                logging.debug(f"Batch fail details: {error_str}")

                # Log error for each record in the failed batch
                for j, record in enumerate(batch):
                    error_count += 1
                    _append_log_row(log_rows, f"batch_{i // batch_size}_row_{j}", "error")

        return {
            "log_rows": log_rows,
            "created_count": created_count,
            "error_count": error_count,
        }

    def _process_upsert_batches(
        self,
        table: Table,
        records: list,
        upsert_key_fields: list,
        batch_size: int,
    ) -> dict:
        """
        Process upsert operations using Airtable's native upsert functionality.

        Args:
            table: Airtable table object
            records: List of record dicts to upsert
            upsert_key_fields: List of key fields for upsert
            batch_size: Maximum records per batch
        Returns:
            Dict with log_rows, created_count, updated_count, error_count
        """
        log_rows = []
        created_count = 0
        updated_count = 0
        error_count = 0

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]

            try:
                # Use Airtable's native upsert via batch_upsert
                upsert_batch = [{"fields": record} for record in batch]
                response = table.batch_upsert(upsert_batch, key_fields=upsert_key_fields)

                # Handle upsert response
                for record in response.get("records", []):
                    record_id = record["id"]
                    _append_log_row(log_rows, record_id, "upsert")

                # Count created/updated
                created_records = response.get("createdRecords", [])
                updated_records = response.get("updatedRecords", [])
                created_count += len(created_records)
                updated_count += len(updated_records)
                logging.debug(f"✅ Upserted batch: {len(created_records)} created, {len(updated_records)} updated")

            except Exception as e:
                error_str = str(e)

                # Check for computed field error
                if "INVALID_VALUE_FOR_COLUMN" in error_str and "field is computed" in error_str:
                    field_name = "unknown"
                    if 'Field "' in error_str:
                        start = error_str.index('Field "') + len('Field "')
                        end = error_str.index('"', start)
                        field_name = error_str[start:end]

                    raise UserException(
                        f"Cannot write to computed field '{field_name}'. "
                        f"Computed fields are automatically calculated by Airtable and cannot accept values. "
                        f"Please remove this field from your column mapping or exclude it from the input data."
                    )

                # Check for duplicate key error
                if "INVALID_VALUE_FOR_COLUMN" in error_str:
                    raise UserException(
                        "Airtable upsert failed: The upsert key fields do not uniquely identify records in the table. "
                        "Please ensure the combination of upsert key fields is unique for all records."
                        f"Airtable error: {error_str}"
                    )
                if "INVALID_RECORDS" in error_str:
                    raise UserException(
                        "Airtable upsert failed: Your input data contains duplicate values for the upsert key. "
                        "Please ensure there are no duplicate records for the upsert key fields in your input data.\n"
                        f"Airtable error: {error_str}"
                    )
                logging.error("Batch failed to upsert records. Use debug for more info.")
                logging.debug(f"Batch fail details: {error_str}")
        return {
            "log_rows": log_rows,
            "created_count": created_count,
            "updated_count": updated_count,
            "error_count": error_count,
        }

    @staticmethod
    def _create_table_from_config(base: Base, table_name: str, column_configs: list) -> Table:
        """
        Create a new Airtable table based on column configuration.

        Args:
            base: Airtable Base object
            table_name: Name of the table to create
            column_configs: List of ColumnConfig objects for custom field types (required)
        Returns:
            Created Table object
        """
        if not column_configs:
            raise UserException(
                "Column configuration is required for table creation. "
                "Please configure columns using the 'Load Columns' button in the UI."
            )

        fields = []
        for col_config in column_configs:
            field_config = {
                "name": col_config.destination_name,
                "type": col_config.dtype,
            }

            if col_config.dtype in ("number", "currency", "percent"):
                # Use default precision: 2 for currency, 0 for others
                precision = 2 if col_config.dtype == "currency" else 0
                field_config["options"] = {"precision": precision}
            elif col_config.dtype == "date":
                field_config["options"] = {"dateFormat": {"name": "iso", "format": "YYYY-MM-DD"}}
            elif col_config.dtype == "dateTime":
                field_config["options"] = {
                    "dateFormat": {"name": "iso", "format": "YYYY-MM-DD"},
                    "timeFormat": {"name": "24hour", "format": "HH:mm"},
                    "timeZone": "utc",
                }
            elif col_config.dtype == "duration":
                field_config["options"] = {"durationFormat": "h:mm:ss"}
            elif col_config.dtype in ("singleSelect", "multipleSelects"):
                field_config["options"] = {"choices": []}

            fields.append(field_config)

        logging.info(f"🆕 Creating new table '{table_name}' with fields: {[f['name'] for f in fields]}")

        try:
            created_table = base.create_table(table_name, fields)
            logging.info(f"✅ Successfully created table '{table_name}'")
            return created_table
        except Exception as e:
            error_msg = f"❌ Failed to create table '{table_name}': {e}"
            logging.error(error_msg)
            raise UserException(
                f"Table creation failed. Please ensure the table '{table_name}' exists in your Airtable base "
                f"or create it manually with the following columns: {[f['name'] for f in fields]}"
            )


# ============================================================================
# MODULE-LEVEL UTILITY FUNCTIONS
# ============================================================================


def _append_log_row(log_rows: list, record_id: str, action: str) -> None:
    """
    Helper to append a log row with current timestamp, record_id, and status.

    Args:
        log_rows: List to append log row to
        record_id: ID of the record
        action: Action performed (create, upsert, error)
    """
    log_rows.append(
        {
            "datetime": datetime.utcnow().isoformat(),
            "record_id": record_id,
            "status": action,
        }
    )
