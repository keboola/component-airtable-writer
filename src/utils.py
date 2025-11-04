"""
Utility functions for Keboola-Airtable integration.
"""

from client_storage import SAPIClient
from configuration import ColumnConfig


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
