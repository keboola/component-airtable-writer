import logging
from datetime import datetime
import pandas as pd
from pyairtable import Api, Table, Base
from typing import Dict, List
from keboola.component.exceptions import UserException
from client_storage import SAPIClient
from configuration import ColumnConfig


def map_records(
    records: List, field_mapping: Dict, column_configs: List = None
) -> List:
    """
    Map input records to Airtable field names using the provided field mapping.
    Note: Input records should already be filtered to only include mappable columns.

    Args:
        records: List of input records (dicts) with only mappable columns.
        field_mapping: Mapping from input column names to Airtable field names.
        column_configs: Optional list of ColumnConfig objects to handle PK numeric conversion.

    Returns:
        List of tuples: (record_id, mapped_record_dict)
    """
    # Build set of numeric PK fields that need string conversion
    pk_numeric_fields = set()
    if column_configs:
        for col in column_configs:
            if col.pk and col.dtype in ["number", "currency", "percent"]:
                pk_numeric_fields.add(col.destination_name)

    mapped_records = []
    for rec in records:
        mapped = {}
        raw_id = rec.get("recordId")

        # Improved recordId handling
        if (
            raw_id is None
            or raw_id == ""
            or (
                isinstance(raw_id, str)
                and raw_id.strip().lower() in ["", "nan", "none"]
            )
            or (pd.notna(raw_id) and pd.isna(raw_id))
        ):
            record_id = None
        else:
            record_id = str(raw_id).strip()

        for k, v in rec.items():
            if k == "recordId":
                continue
            # All columns should be mappable at this point
            airtable_field = field_mapping[k]
            # Handle None/NaN values properly
            if pd.isna(v) or v is None:
                mapped[airtable_field] = None
            # Convert numeric PK values to strings due to API constraints
            elif airtable_field in pk_numeric_fields and isinstance(v, (int, float)):
                mapped[airtable_field] = str(v)
            else:
                mapped[airtable_field] = v

        mapped_records.append((record_id, mapped))
    return mapped_records


def process_records_batch(
    table: Table,
    mapped_records: List,
    load_type: str,
    upsert_key_fields: List = None,
    batch_size: int = 10,
) -> None:
    """
    Process records using Airtable's batch API, branching by load_type.

    Args:
        table: Airtable table object
        mapped_records: List of (record_id, record_data) tuples
        load_type: One of 'Full Load', 'Incremental Load', 'Append'
        upsert_key_fields: Fields to use for upsert matching (for Incremental Load)
        batch_size: Maximum records per batch (Airtable limit is 10)
    """
    log_rows = []
    total_processed = 0
    total_created = 0
    total_updated = 0
    total_errors = 0

    # For all load types, ignore record_id (Keboola does not persist them)
    records = [record_data for _, record_data in mapped_records]

    if load_type == "Full Load":
        # Table should already be cleared before this is called
        logging.info(f"Processing {len(records)} records as creates (full load)")
        create_results = process_create_batches(table, records, batch_size)
        log_rows.extend(create_results["log_rows"])
        total_created += create_results["created_count"]
        total_errors += create_results["error_count"]
        total_processed += len(records)

    elif load_type == "Incremental Load":
        logging.info(
            f"Processing {len(records)} records as upserts (Incremental Load) using keys: {upsert_key_fields}"
        )
        upsert_results = process_upsert_batches(
            table, records, upsert_key_fields, batch_size
        )
        log_rows.extend(upsert_results["log_rows"])
        total_created += upsert_results["created_count"]
        total_updated += upsert_results.get("updated_count", 0)
        total_errors += upsert_results["error_count"]
        total_processed += len(records)

    elif load_type == "Append":
        logging.info(f"Processing {len(records)} records as creates (Append mode)")
        create_results = process_create_batches(table, records, batch_size)
        log_rows.extend(create_results["log_rows"])
        total_created += create_results["created_count"]
        total_errors += create_results["error_count"]
        total_processed += len(records)

    else:
        raise UserException(f"Unknown load_type: {load_type}")

    # Log final summary
    logging.info(
        f"📈 Batch processing complete: {total_created} created, "
        f"{total_updated} updated, {total_errors} errors from {total_processed} total records"
    )


def process_create_batches(table: Table, records: List, batch_size: int) -> Dict:
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
            # Use pyairtable's batch_create method with typecast for auto-conversion
            created_records = table.batch_create(batch, typecast=True)

            for record in created_records:
                record_id = record["id"]
                created_count += 1
                _append_log_row(log_rows, record_id, "create")
            logging.debug(f"✅ Created batch of {len(batch)} records")

        except Exception as e:
            logging.error("Batch failed to insert records. Use debug for more info.")
            logging.debug(f"Batch fail details: {str(e)}")

            # Log error for each record in the failed batch
            for j, record in enumerate(batch):
                error_count += 1
                _append_log_row(log_rows, f"batch_{i // batch_size}_row_{j}", "error")

    return {
        "log_rows": log_rows,
        "created_count": created_count,
        "error_count": error_count,
    }


def process_upsert_batches(
    table: Table,
    records: List,
    upsert_key_fields: List,
    batch_size: int,
) -> Dict:
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
            response = table.batch_upsert(
                upsert_batch, key_fields=upsert_key_fields, typecast=True
            )

            # Handle upsert response
            for record in response.get("records", []):
                record_id = record["id"]
                _append_log_row(log_rows, record_id, "upsert")

            # Count created/updated
            created_count += len(response.get("createdRecords", []))
            updated_count += len(response.get("updatedRecords", []))
            logging.debug(
                f"✅ Upserted batch: {len(response.get('createdRecords', []))} created,"
                f"{len(response.get('updatedRecords', []))} updated"
            )

        except Exception as e:
            if "INVALID_VALUE_FOR_COLUMN" in str(e):
                raise UserException(
                    "Airtable upsert failed: The upsert key fields do not uniquely identify records in the table. "
                    "Please ensure the combination of upsert key fields is unique for all records."
                    f"Airtable error: {str(e)}"
                )
            if "INVALID_RECORDS" in str(e):
                raise UserException(
                    "Airtable upsert failed: Your input data contains duplicate values for the upsert key. "
                    "Please ensure there are no duplicate records for the upsert key fields in your input data.\n"
                    f"Airtable error: {str(e)}"
                )

    return {
        "log_rows": log_rows,
        "created_count": created_count,
        "updated_count": updated_count,
        "error_count": error_count,
    }


def build_field_mapping(column_configs: List) -> Dict[str, str]:
    """
    Build field mapping from column configurations.
    Args:
        column_configs: List of ColumnConfig objects
    Returns:
        Dict mapping source names to destination names
    """
    if not column_configs:
        raise UserException("Column configuration is required.")
    computed_fields = ["Total billed"]
    return {
        col.source_name: col.destination_name
        for col in column_configs
        if col.destination_name not in computed_fields
    }


def clear_table_for_full_load(table: Table):
    """
    Delete all records from the table in preparation for a full load operation.

    Args:
        table: Airtable table object
    """
    try:
        # Get all record IDs
        all_records = table.all(fields=[])  # Only get IDs, no field data
        record_ids = [record["id"] for record in all_records]

        if not record_ids:
            logging.info("📭 Table is already empty, nothing to delete")
            return

        logging.info(f"🗑️ Full load: Deleting {len(record_ids)} existing records")

        # Delete in batches (Airtable limit is 10 per batch)
        batch_size = 10
        for i in range(0, len(record_ids), batch_size):
            batch_ids = record_ids[i : i + batch_size]
            table.batch_delete(batch_ids)
            logging.debug(f"✅ Deleted batch of {len(batch_ids)} records")

        logging.info(f"✅ Successfully deleted all {len(record_ids)} records")

    except Exception as e:
        logging.error(f"❌ Failed to delete records for full load: {e}")
        raise


def get_primary_key_fields(column_configs: List) -> List[str]:
    """
    Extract primary key field names from column configurations.

    Args:
        column_configs: List of ColumnConfig objects
    Returns:
        List of destination field names marked as primary keys
    """
    pk_fields = []
    for col_config in column_configs:
        if col_config.pk:
            pk_fields.append(col_config.destination_name)
    return pk_fields


# ============================================================================
# TABLE MANAGEMENT FUNCTIONS
# ============================================================================


def detect_number_precision(df: pd.DataFrame, column_name: str) -> int:
    """
    Detect the appropriate precision for a number field based on actual data.

    Args:
        df: DataFrame containing the data
        column_name: Name of the column to analyze

    Returns:
        Precision value (0-8) representing decimal places needed
    """
    if column_name not in df.columns:
        return 0  # Default to integer

    # Get non-null values
    values = df[column_name].dropna()

    if len(values) == 0:
        return 0

    # Check if all values are integers
    if pd.api.types.is_integer_dtype(values):
        return 0

    # For float types, determine max decimal places needed
    max_precision = 0
    for val in values:
        if pd.notna(val):
            # Convert to string and count decimal places
            str_val = str(float(val))
            if "." in str_val:
                decimal_places = len(str_val.split(".")[1].rstrip("0"))
                max_precision = max(max_precision, decimal_places)

    # Cap at 8 (Airtable's maximum)
    return min(max_precision, 8)


def get_or_create_table(
    api: Api,
    base_id: str,
    table_name: str,
    input_df: pd.DataFrame,
    column_configs: List = None,
) -> Table:
    """
    Get an existing table by name, or create it if it doesn't exist.
    If creating, use the input DataFrame to infer the table schema.

    Args:
        api: Airtable API instance
        base_id: Airtable base ID
        table_name: Table name
        input_df: Input DataFrame to infer schema if table needs to be created
        column_configs: Optional list of ColumnConfig objects for custom field types
    Returns:
        Table object
    """
    base = api.base(base_id)

    # Try to get existing table by checking if it exists in the base schema
    try:
        base_schema = base.schema()
        existing_table_names = [table.name for table in base_schema.tables]

        if table_name in existing_table_names:
            logging.info(f"📋 Found existing table '{table_name}'")
            return base.table(table_name)
        else:
            logging.info(
                f"📋 Table '{table_name}' not found in base. Will create new table."
            )
            # Create new table with schema from input data
            return create_table_from_dataframe(
                base, table_name, input_df, column_configs
            )

    except Exception as e:
        logging.error(f"❌ Failed to check base schema or create table: {e}")
        raise UserException(f"Failed to access or create table '{table_name}': {e}")


def create_table_from_dataframe(
    base: Base, table_name: str, df: pd.DataFrame, column_configs: List = None
) -> Table:
    """
    Create a new Airtable table based on DataFrame schema.
    NOTE: Table creation via API requires specific permissions and may not be available in all plans.

    Args:
        base: Airtable Base object
        table_name: Name of the table to create
        df: DataFrame to infer schema from
        column_configs: List of ColumnConfig objects for custom field types (required)
    Returns:
        Created Table object
    """
    if not column_configs:
        raise UserException(
            "Column configuration is required for table creation. "
            "Please configure columns using the 'Load Columns' button in the UI."
        )

    # Airtable requires the first field to be one of: singleLineText, email, url, phoneNumber, autoNumber
    VALID_PRIMARY_TYPES = [
        "singleLineText",
        "email",
        "url",
        "phoneNumber",
        "autoNumber",
    ]

    # Reorder fields: Put PK fields first (if any), then remaining fields
    # This ensures the most important identifier becomes Airtable's primary field
    pk_configs = [col for col in column_configs if col.pk]
    non_pk_configs = [col for col in column_configs if not col.pk]

    # Use PK-first ordering if PKs exist, otherwise keep original order
    ordered_configs = (pk_configs + non_pk_configs) if pk_configs else column_configs

    if pk_configs:
        pk_names = [col.destination_name for col in pk_configs]
        logging.info(f"📌 Reordering fields: PK fields {pk_names} will be placed first")

    fields = []
    for idx, col_config in enumerate(ordered_configs):
        field_config = {
            "name": col_config.destination_name,
            "type": col_config.dtype,
        }

        # Check if this is the first field and if it needs type conversion
        is_first_field = idx == 0

        if is_first_field and col_config.dtype not in VALID_PRIMARY_TYPES:
            # First field must be a valid Airtable primary type
            original_type = col_config.dtype
            field_config["type"] = "singleLineText"
            logging.warning(
                f"⚠️ First field '{col_config.destination_name}'"
                f"type '{original_type}' is not valid for Airtable primary field."
                f"Converting to 'singleLineText'. Consider using a valid primary type "
                f"({', '.join(VALID_PRIMARY_TYPES)}) for this field."
            )
        elif col_config.dtype == "number":
            precision = detect_number_precision(df, col_config.source_name)
            precision = precision if precision > 0 else 0
            field_config["options"] = {"precision": precision}
        elif col_config.dtype == "currency":
            precision = detect_number_precision(df, col_config.source_name)
            precision = precision if precision > 0 else 2
            field_config["options"] = {"precision": precision}
        elif col_config.dtype == "percent":
            precision = detect_number_precision(df, col_config.source_name)
            field_config["options"] = {"precision": precision}
        elif col_config.dtype == "date":
            field_config["options"] = {
                "dateFormat": {"name": "iso", "format": "YYYY-MM-DD"}
            }
        elif col_config.dtype == "dateTime":
            field_config["options"] = {
                "dateFormat": {"name": "iso", "format": "YYYY-MM-DD"},
                "timeFormat": {"name": "24hour", "format": "HH:mm"},
                "timeZone": "utc",
            }
        elif col_config.dtype == "duration":
            field_config["options"] = {"durationFormat": "h:mm:ss"}
        elif col_config.dtype == "singleSelect":
            field_config["options"] = {"choices": []}
        elif col_config.dtype == "multipleSelects":
            field_config["options"] = {"choices": []}

        fields.append(field_config)

    logging.info(
        f"🆕 Creating new table '{table_name}' with fields: {[f['name'] for f in fields]}"
    )

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


def get_table_schema(table: Table) -> Dict:
    """
    Get the current schema/configuration of an existing table using proper table metadata.

    Args:
        table: Airtable table object
    Returns:
        Dict with schema information
    """
    try:
        # Use table.schema() to get actual table metadata instead of sampling records
        table_schema = table.schema()

        schema = {
            "table_name": table_schema.name,
            "fields": [field.name for field in table_schema.fields],
            "field_types": {field.name: field.type for field in table_schema.fields},
            "primary_field_id": table_schema.primary_field_id,
        }

        logging.info(
            f"📋 Retrieved schema for table '{table_schema.name}': {schema['fields']}"
        )
        return schema

    except Exception as e:
        logging.error(f"❌ Failed to get table schema: {e}")
        raise


def compare_schemas(input_df: pd.DataFrame, table_schema: Dict) -> Dict:
    """
    Compare input DataFrame schema with existing table schema.

    Args:
        input_df: Input DataFrame
        table_schema: Table schema dict
    Returns:
        Dict with missing fields, common fields, and update flag
    """
    input_columns = set(input_df.columns) - {"recordId"}  # Exclude recordId
    table_fields = set(table_schema.get("fields", []))

    missing_in_table = input_columns - table_fields
    missing_in_input = table_fields - input_columns
    common_fields = input_columns & table_fields

    comparison = {
        "missing_in_table": list(missing_in_table),
        "missing_in_input": list(missing_in_input),
        "common_fields": list(common_fields),
        "needs_update": len(missing_in_table) > 0,
    }

    return comparison


# --------------------------------------------------------------------------
# Batch-processing helper (for DRY logging)
# --------------------------------------------------------------------------
def _append_log_row(log_rows, record_id, status):
    """
    Helper to append a log row with current timestamp, record_id, and status.
    """
    log_rows.append(
        {
            "datetime": datetime.utcnow().isoformat(),
            "record_id": record_id,
            "status": status,
        }
    )


def map_to_airtable_type(keboola_type: str) -> str:
    """
    Maps Keboola data types to Airtable field types.

    Args:
        keboola_type: The Keboola data type

    Returns:
        str: Corresponding Airtable field type
    """
    type_mapping = {
        "STRING": "singleLineText",
        "INTEGER": "number",
        "NUMERIC": "number",
        "FLOAT": "number",
        "BOOLEAN": "checkbox",
        "DATE": "date",
        "TIMESTAMP": "dateTime",
    }
    return type_mapping.get(keboola_type, "singleLineText")


def get_sapi_column_definition(table_id: str, storage_url: str, storage_token: str):
    """
    Get column definitions from Storage API table metadata.

    Args:
        table_id: Storage table ID
        storage_url: Storage API URL
        storage_token: Storage API token

    Returns:
        List of column configuration dicts with Keboola data types
    """
    storage_client = SAPIClient(storage_url, storage_token)
    table_detail = storage_client.get_table_detail(table_id)
    columns = []

    if table_detail.get("isTyped") and table_detail.get("definition"):
        primary_keys = set(table_detail["definition"].get("primaryKeysNames", []))
        columns_to_process = [
            {
                "name": column["name"],
                "dtype": column["definition"].get("type", "STRING"),
            }
            for column in table_detail["definition"]["columns"]
        ]
    else:  # non-typed table
        primary_keys = set(table_detail.get("primaryKey", []))
        columns_to_process = [
            {
                "name": col_name,
                "dtype": "STRING",
            }
            for col_name in table_detail.get("columns", [])
        ]

    # Create column configs for all columns
    for col_info in columns_to_process:
        col_name = col_info["name"]
        columns.append(
            ColumnConfig(
                source_name=col_name,
                destination_name=col_name,
                dtype=col_info["dtype"],
                pk=col_name in primary_keys,
            ).model_dump()
        )
    return columns
