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
    clear_table_for_full_load,
    get_primary_key_fields,
    validate_field_mapping,
    validate_connection,
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
        logging.info(f"Loaded input data: {len(df)} rows")

        try:
            validate_connection(params.api_token, params.base_id)

            table = get_or_create_table(
                params.api_token,
                params.base_id,
                params.table_name,
                df,
                params.destination.columns,
            )

            # Get field mapping for the table
            field_mapping = fetch_field_mapping(params.destination.columns)

            # Validate field mapping and get mappable columns
            mappable_columns = validate_field_mapping(list(df.columns), field_mapping)

            # Filter DataFrame to only include mappable columns
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

            load_type = params.destination.load_type
            upsert_key_fields = None
            # Determine upsert key fields based on load type
            if load_type == "Incremental Load":
                upsert_key_fields = get_primary_key_fields(params.destination.columns)
                if not upsert_key_fields:
                    raise UserException(
                        "Incremental load requires at least one primary key field to be set in the configuration."
                    )
            elif params.destination.load_type == "Full Load":
                clear_table_for_full_load(table)
                upsert_key_fields = None
            elif params.destination.load_type == "Append":
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
