import logging
from datetime import datetime
import pandas as pd
from pyairtable import Api, Table, Base
from typing import Dict, List
from keboola.component.exceptions import UserException


def map_records(records: List, field_mapping: Dict) -> List:
    """
    Map input records to Airtable field names using the provided field mapping.

    Args:
        records: List of input records (dicts).
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
            airtable_field = field_mapping.get(k)
            if airtable_field:
                # Handle None/NaN values properly
                if pd.isna(v) or v is None:
                    mapped[airtable_field] = None
                else:
                    mapped[airtable_field] = v
            else:
                logging.warning(f"⚠️ No match found in Airtable for input column: '{k}'")
        mapped_records.append((record_id, mapped))
    return mapped_records


def process_records_batch(
    table: Table,
    mapped_records: List,
    upsert_key_fields: List = None,
    batch_size: int = 10,
) -> None:
    """
    Process records using Airtable's native batch API with built-in upsert support.
    Always uses batch operations even for single records for consistency and performance.

    Args:
        table: Airtable table object
        mapped_records: List of (record_id, record_data) tuples
        upsert_key_fields: Fields to use for upsert matching (uses Airtable's native upsert)
        batch_size: Maximum records per batch (Airtable limit is 10)
    """
    log_rows = []

    # Split records into creates and updates
    records_to_create = []
    records_to_update = []

    for record_id, record_data in mapped_records:
        if record_id:
            # Has recordId - update operation
            records_to_update.append({"id": record_id, "fields": record_data})
        else:
            # No recordId - create operation (or upsert if key fields specified)
            records_to_create.append({"fields": record_data})

    total_processed = 0
    total_created = 0
    total_updated = 0
    total_errors = 0

    # Process updates in batches
    if records_to_update:
        logging.info(
            f"Processing {len(records_to_update)} updates in batches of {batch_size}"
        )
        update_results = process_update_batches(table, records_to_update, batch_size)
        log_rows.extend(update_results["log_rows"])
        total_updated += update_results["updated_count"]
        total_errors += update_results["error_count"]
        total_processed += len(records_to_update)

    # Process creates in batches (with optional upsert)
    if records_to_create:
        if upsert_key_fields:
            logging.info(
                f"Processing {len(records_to_create)} upserts using fields: {upsert_key_fields}"
            )
            create_results = process_upsert_batches(
                table, records_to_create, upsert_key_fields, batch_size
            )
        else:
            logging.info(
                f"Processing {len(records_to_create)} creates in batches of {batch_size}"
            )
            create_results = process_create_batches(
                table, records_to_create, batch_size
            )

        log_rows.extend(create_results["log_rows"])
        total_created += create_results["created_count"]
        total_updated += create_results.get("updated_count", 0)  # For upserts
        total_errors += create_results["error_count"]
        total_processed += len(records_to_create)

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
            # Note: This may require using table.batch_update with performUpsert=True

            # Call the API directly since pyairtable might not have batch_upsert
            response = table.batch_update(
                batch, upsert=upsert_key_fields, typecast=True
            )

            # Handle upsert response
            for record in response:
                record_id = record["id"]
                # Airtable doesn't clearly indicate if it was create or update in standard response
                # We'll mark as "upsert" for clarity
                _append_log_row(log_rows, record_id, "upsert")

            # For summary, assume all are creates (conservative estimate)
            created_count += len(batch)
            logging.debug(f"✅ Upserted batch of {len(batch)} records")

        except Exception as e:
            logging.error(f"❌ Batch upsert failed: {str(e)}", exc_info=True)

            # Fallback: try individual operations or log as errors
            for j, record in enumerate(batch):
                error_count += 1
                _append_log_row(log_rows, f"batch_{i // batch_size}_row_{j}", "error")

    return {
        "log_rows": log_rows,
        "created_count": created_count,
        "updated_count": updated_count,
        "error_count": error_count,
    }


def build_upsert_lookup_map(table: Table, key_fields: List) -> Dict:
    """
    Build a lookup map of existing records using specified key fields.

    Args:
        table: Airtable table object
        key_fields: List of key field names
    Returns:
        Dict where keys are tuples of key field values, values are record IDs.
    """
    lookup_map = {}

    try:
        # Fetch existing records with pagination for large tables
        all_records = []
        offset = None
        page_size = 100  # Airtable's max page size

        while True:
            if offset:
                records_page = table.all(page_size=page_size, offset=offset)
            else:
                records_page = table.all(page_size=page_size)

            if not records_page:
                break

            all_records.extend(records_page)

            # Check if there are more records
            if len(records_page) < page_size:
                break

            # Get offset for next page (this depends on pyairtable implementation)
            try:
                offset = records_page[-1]["id"]
            except Exception:
                break  # Fallback if pagination doesn't work as expected

        logging.info(
            f"📋 Fetched {len(all_records)} existing records for upsert lookup"
        )

        for record in all_records:
            record_id = record["id"]
            fields = record.get("fields", {})

            # Build key tuple from specified key fields
            key_values = []
            has_all_keys = True

            for key_field in key_fields:
                value = fields.get(key_field)
                if value is None or value == "":
                    has_all_keys = False
                    break
                key_values.append(str(value).strip())

            if has_all_keys:
                key_tuple = tuple(key_values)
                if key_tuple in lookup_map:
                    logging.warning(
                        f"⚠️ Duplicate key found: {key_tuple}. Will use first occurrence."
                    )
                else:
                    lookup_map[key_tuple] = record_id

        logging.info(
            f"🗂️ Built lookup map with {len(lookup_map)} unique key combinations"
        )
        return lookup_map

    except Exception as e:
        logging.error(f"❌ Failed to build upsert lookup map: {e}")
        # Fallback to simple approach without pagination
        try:
            simple_records = table.all(max_records=1000)  # Limit to prevent timeouts
            logging.warning("⚠️ Using fallback pagination with max 1000 records")
            for record in simple_records:
                record_id = record["id"]
                fields = record.get("fields", {})

                key_values = []
                has_all_keys = True

                for key_field in key_fields:
                    value = fields.get(key_field)
                    if value is None or value == "":
                        has_all_keys = False
                        break
                    key_values.append(str(value).strip())

                if has_all_keys:
                    key_tuple = tuple(key_values)
                    lookup_map[key_tuple] = record_id

            return lookup_map
        except Exception:
            return {}


def find_existing_record_by_keys(
    record: Dict, lookup_map: Dict, key_fields: List
) -> str | None:
    """
    Find existing record ID by matching key field values.

    Args:
        record: Input record dict
        lookup_map: Lookup map from build_upsert_lookup_map
        key_fields: List of key field names
    Returns:
        Record ID if found, else None
    """
    try:
        key_values = []
        for key_field in key_fields:
            value = record.get(key_field)
            if value is None or value == "":
                return None  # Missing key field value
            key_values.append(str(value).strip())

        key_tuple = tuple(key_values)
        return lookup_map.get(key_tuple)
    except Exception as e:
        logging.debug(f"Error finding existing record by keys: {e}")
        return None


def update_lookup_map_with_new_record(
    lookup_map: Dict, record: Dict, record_id: str, key_fields: List
) -> None:
    """
    Update the lookup map with a newly created record to enable upserts within the same batch.

    Args:
        lookup_map: Lookup map to update
        record: Record dict
        record_id: Record ID to associate
        key_fields: List of key field names
    """
    try:
        key_values = []
        for key_field in key_fields:
            value = record.get(key_field)
            if value is not None and value != "":
                key_values.append(str(value).strip())
            else:
                return  # Can't index record without all key fields

        key_tuple = tuple(key_values)
        lookup_map[key_tuple] = record_id
    except Exception as e:
        logging.debug(f"Error updating lookup map: {e}")


def fetch_field_mapping(table: Table, input_df: pd.DataFrame = None):
    """
    Fetch field mapping and computed fields from the Airtable table.

    Args:
        table: Airtable table object
        input_df: Optional input DataFrame (used if table schema is unavailable)
    Returns:
        Tuple of (field mapping dict, list of computed fields)
    """
    try:
        # Use table schema to get field information directly from metadata
        table_schema = table.schema()
        airtable_fields = [field.name for field in table_schema.fields]
        logging.info(f"📋 Columns available in table '{table.name}': {airtable_fields}")
    except Exception as e:
        # Fallback to input data for newly created tables or schema access issues
        logging.info(
            f"📋 Could not access table schema: {e}. Building field mapping from input data."
        )
        if input_df is not None:
            # For newly created tables, the field names should match the DataFrame columns
            # (excluding recordId which is handled by Airtable)
            airtable_fields = [col for col in input_df.columns if col != "recordId"]
            logging.info(f"📋 Using input columns as field names: {airtable_fields}")
        else:
            logging.warning(
                "⚠️ No input data provided and cannot access table schema. Cannot build field mapping."
            )
            return {}, []

    # Hardcoded list of computed (non-editable) fields
    computed_fields = ["Total billed"]
    logging.info(
        f"⏭️ Computed fields (will be skipped if present in input): {computed_fields}"
    )

    # Use direct 1:1 mapping: field name to itself, skip computed fields
    mapping = {
        field: field for field in airtable_fields if field not in computed_fields
    }

    logging.info(f"🧭 Field mapping will be used: {mapping}")
    return mapping, computed_fields


def validate_connection(api_token: str, base_id: str, table_name: str) -> Table:
    """
    Connect to Airtable using the API token and check access to the base and table.

    Args:
        api_token: Airtable API token
        base_id: Airtable base ID
        table_name: Table name
    Returns:
        Table object if connection is successful
    """
    try:
        api = Api(api_token)
        base = api.base(base_id)
        table = base.table(table_name)

        # Fetch 1 test record to validate connection
        table.all(max_records=1)
        logging.info("✅ Successfully connected to Airtable.")

        return table
    except Exception:
        logging.error(
            "❌ Failed to validate Airtable credentials or access.", exc_info=True
        )
        raise


# ============================================================================
# TABLE MANAGEMENT FUNCTIONS
# ============================================================================


def get_or_create_table(
    api_token: str, base_id: str, table_name: str, input_df: pd.DataFrame
) -> Table:
    """
    Get an existing table by name, or create it if it doesn't exist.
    If creating, use the input DataFrame to infer the table schema.

    Args:
        api_token: Airtable API token
        base_id: Airtable base ID
        table_name: Table name
        input_df: Input DataFrame to infer schema if table needs to be created
    Returns:
        Table object
    """
    api = Api(api_token)
    base = api.base(base_id)

    # Try to get existing table
    try:
        table = base.table(table_name)
        # Test access to the table
        table.all(max_records=1)
        logging.info(f"✅ Found existing table '{table_name}'")
        return table
    except Exception as e:
        logging.info(
            f"📋 Table '{table_name}' not found or inaccessible. Will create new table."
        )
        logging.debug(f"Error details: {e}")

        # Create new table with schema from input data
        return create_table_from_dataframe(base, table_name, input_df)


def create_table_from_dataframe(base: Base, table_name: str, df: pd.DataFrame) -> Table:
    """
    Create a new Airtable table based on DataFrame schema.
    NOTE: Table creation via API requires specific permissions and may not be available in all plans.

    Args:
        base: Airtable Base object
        table_name: Name of the table to create
        df: DataFrame to infer schema from
    Returns:
        Created Table object
    """
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
    log_rows.append({
        "datetime": datetime.utcnow().isoformat(),
        "record_id": record_id,
        "status": status,
    })
