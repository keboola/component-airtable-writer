import logging
from datetime import datetime
import pandas as pd
from pyairtable import Api, Table, Base
from typing import Dict, List
from keboola.component.exceptions import UserException


def validate_field_mapping(input_columns: List[str], field_mapping: Dict) -> List[str]:
    """
    Validate field mapping and return list of mappable columns.

    Args:
        input_columns: List of input column names
        field_mapping: Mapping from input column names to Airtable field names

    Returns:
        List of column names that can be mapped (includes recordId if present)
    """
    unmapped_columns = []
    mappable_columns = []

    for col in input_columns:
        if col == "recordId" or col in field_mapping:
            mappable_columns.append(col)
        else:
            unmapped_columns.append(col)

    if unmapped_columns:
        logging.debug(f"🔍 Columns not mapped and will be skipped: {unmapped_columns}")

    return mappable_columns


def map_records(records: List, field_mapping: Dict) -> List:
    """
    Map input records to Airtable field names using the provided field mapping.
    Note: Input records should already be filtered to only include mappable columns.

    Args:
        records: List of input records (dicts) with only mappable columns.
        field_mapping: Mapping from input column names to Airtable field names.

    Returns:
        List of tuples: (record_id, mapped_record_dict)
    """
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
            # Use pyairtable's batch_create method
            created_records = table.batch_create(batch)

            for record in created_records:
                record_id = record["id"]
                created_count += 1
                _append_log_row(log_rows, record_id, "create")
            logging.debug(f"✅ Created batch of {len(batch)} records")

        except Exception as e:
            logging.error(f"❌ Batch create failed: {str(e)}", exc_info=True)

            # Log error for each record in the failed batch
            for j, record in enumerate(batch):
                error_count += 1
                _append_log_row(log_rows, f"batch_{i // batch_size}_row_{j}", "error")

    return {
        "log_rows": log_rows,
        "created_count": created_count,
        "error_count": error_count,
    }


def process_update_batches(table: Table, records: List, batch_size: int) -> Dict:
    """
    Process update operations in batches using Airtable's batch update API.

    Args:
        table: Airtable table object
        records: List of record dicts to update
        batch_size: Maximum records per batch
    Returns:
        Dict with log_rows, updated_count, error_count
    """
    log_rows = []
    updated_count = 0
    error_count = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]

        try:
            # Use pyairtable's batch_update method
            updated_records = table.batch_update(batch)

            for record in updated_records:
                record_id = record["id"]
                updated_count += 1
                _append_log_row(log_rows, record_id, "update")

            logging.debug(f"✅ Updated batch of {len(batch)} records")

        except Exception as e:
            logging.error(f"❌ Batch update failed: {str(e)}", exc_info=True)

            # Log error for each record in the failed batch
            for record in batch:
                error_count += 1
                _append_log_row(log_rows, record.get("id", "unknown"), "error")

    return {
        "log_rows": log_rows,
        "updated_count": updated_count,
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


def fetch_field_mapping(column_configs: List):
    """
    Fetch field mapping and computed fields from the Airtable table.

    Args:
        column_configs: List of ColumnConfig objects for custom mapping
    Returns:
        field mapping dict
    """
    if not column_configs:
        raise UserException("Column configuration is required for field mapping.")

    # Hardcoded list of computed (non-editable) fields
    computed_fields = ["Total billed"]
    mapping = {
        col_config.source_name: col_config.destination_name
        for col_config in column_configs
        if col_config.destination_name not in computed_fields
    }
    return mapping


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


def validate_connection(api_token: str, base_id: str) -> None:
    """
    Connect to Airtable using the API token and check access to the base.
    Does not validate table existence since tables may be created dynamically.

    Args:
        api_token: Airtable API token
        base_id: Airtable base ID
    """

    logging.info(f"Validating connection to Airtable base '{base_id}'")
    try:
        api = Api(api_token)
        base = api.base(base_id)
        # Just check that we can access the base - don't check for specific table
        # since we support table creation
        _ = base.schema()
    except Exception as e:
        raise UserException(f"Failed to connect to Airtable base '{base_id}': {e}")
    logging.info(f"✅ Successfully connected to Airtable base '{base_id}'")


# ============================================================================
# TABLE MANAGEMENT FUNCTIONS
# ============================================================================


def get_or_create_table(
    api_token: str,
    base_id: str,
    table_name: str,
    input_df: pd.DataFrame,
    column_configs: List = None,
) -> Table:
    """
    Get an existing table by name, or create it if it doesn't exist.
    If creating, use the input DataFrame to infer the table schema.

    Args:
        api_token: Airtable API token
        base_id: Airtable base ID
        table_name: Table name
        input_df: Input DataFrame to infer schema if table needs to be created
        column_configs: Optional list of ColumnConfig objects for custom field types
    Returns:
        Table object
    """
    api = Api(api_token)
    base = api.base(base_id)

    # Try to get existing table
    try:
        return base.table(table_name)
    except Exception as e:
        logging.info(
            f"📋 Table '{table_name}' not found or inaccessible. Will create new table."
        )
        logging.debug(f"Error details: {e}")

        # Create new table with schema from input data
        return create_table_from_dataframe(base, table_name, input_df, column_configs)


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
        column_configs: Optional list of ColumnConfig objects for custom field types
    Returns:
        Created Table object
    """
    if column_configs:
        # Use custom column configurations
        fields = []
        for col_config in column_configs:
            field_config = {
                "name": col_config.destination_name,
                "type": col_config.dtype,
            }
            fields.append(field_config)
    else:
        # Fallback to automatic inference
        fields = infer_airtable_fields_from_dataframe(df)

    # Ensure at least one field exists and first field can be primary
    if not fields:
        raise UserException("Cannot create table: no valid fields found in input data")

    # Ensure first field is a valid primary field type
    primary_field_types = [
        "singleLineText",
        "email",
        "url",
        "phoneNumber",
        "autoNumber",
    ]
    if fields[0]["type"] not in primary_field_types:
        # Insert a default primary field at the beginning
        fields.insert(0, {"name": "Primary", "type": "singleLineText"})

    logging.info(
        f"🆕 Creating new table '{table_name}' with fields: {[f['name'] for f in fields]}"
    )

    table_config = {"name": table_name, "fields": fields}

    try:
        # Use pyairtable's native create_table method
        created_table = base.create_table(table_config)
        logging.info(f"✅ Successfully created table '{table_name}'")
        return created_table

    except Exception as e:
        error_msg = f"❌ Failed to create table '{table_name}': {e}"
        logging.error(error_msg)
        raise UserException(
            f"Table creation failed. Please ensure the table '{table_name}' exists in your Airtable base "
            f"or create it manually with the following columns: {[f['name'] for f in fields]}"
        )


def infer_airtable_field_type(series: pd.Series) -> str:
    """
    Infer the most appropriate Airtable field type for a pandas Series.

    Args:
        series: Pandas Series to infer type from
    Returns:
        Airtable field type as string
    """
    # Remove null values for type inference
    non_null_series = series.dropna()

    if len(non_null_series) == 0:
        return "singleLineText"  # Default for empty columns

    # Check for numeric types
    if pd.api.types.is_numeric_dtype(non_null_series):
        if pd.api.types.is_integer_dtype(non_null_series):
            return "number"
        else:
            return "number"  # Float numbers

    # Check for datetime
    if pd.api.types.is_datetime64_any_dtype(non_null_series):
        return "dateTime"

    # Check for boolean
    if pd.api.types.is_bool_dtype(non_null_series):
        return "checkbox"

    # For text fields, check if it should be single select (limited unique values)
    if non_null_series.dtype == "object":
        unique_count = non_null_series.nunique()
        total_count = len(non_null_series)

        # If less than 20 unique values and they represent < 80% of total, likely categorical
        if unique_count <= 20 and (unique_count / total_count) < 0.8:
            return "singleSelect"

        # Check for long text (average length > 100 characters)
        avg_length = non_null_series.str.len().mean()
        if avg_length > 100:
            return "multilineText"

    # Default to single line text
    return "singleLineText"


def infer_airtable_fields_from_dataframe(df: pd.DataFrame) -> List[Dict]:
    """
    Infer Airtable field definitions from pandas DataFrame.

    Args:
        df: Input DataFrame
    Returns:
        List of Airtable field definition dicts
    """
    fields = []

    for column in df.columns:
        if column == "recordId":
            continue  # Skip recordId as it's handled by Airtable

        # Infer field type from data
        field_type = infer_airtable_field_type(df[column])

        field_config = {"name": column, "type": field_type}

        # Add specific configurations for certain field types
        if field_type == "singleSelect":
            # Get unique values for single select options
            unique_values = df[column].dropna().unique()
            field_config["options"] = {
                "choices": [
                    {"name": str(val)} for val in unique_values[:50]
                ]  # Limit to 50 options
            }

        fields.append(field_config)

    return fields


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
