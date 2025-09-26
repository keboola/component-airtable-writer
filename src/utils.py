import csv
import logging
from datetime import datetime
import os
import pandas as pd

def map_records(records: list, field_mapping: dict, computed_fields: list) -> list:
    mapped_records = []
    for rec in records:
        mapped = {}
        raw_id = rec.get("recordId")
        if pd.isna(raw_id) or str(raw_id).strip().lower() in ["", "nan", "none"]:
            record_id = None
        else:
            record_id = str(raw_id).strip()
        for k, v in rec.items():
            if k == "recordId":
                continue
            normalized = normalize_column_name(k)
            airtable_field = field_mapping.get(normalized)
            if airtable_field:
                mapped[airtable_field] = v
            else:
                if normalized in [normalize_column_name(f) for f in computed_fields]:
                    logging.debug(f"⏭️ Skipping computed field: '{k}'")
                else:
                    logging.debug(f"⚠️ No match found in Airtable for input column: '{k}'")
        mapped_records.append((record_id, mapped))
    return mapped_records

def process_records(table, mapped_records: list) -> list:
    log_rows = []
    job_timestamp = datetime.utcnow().isoformat()
    for i, (record_id, record) in enumerate(mapped_records):
        try:
            if record_id:
                updated = table.update(record_id, record)
                logging.info(f"✅ Updated record ID: {updated['id']}")
                log_rows.append({
                    "datetime": job_timestamp,
                    "record_id": record_id,
                    "status": "update",
                    "message": f"Record {record_id} updated successfully."
                })
            else:
                created = table.create(record)
                logging.info(f"✅ Created record ID: {created['id']}")
                log_rows.append({
                    "datetime": job_timestamp,
                    "record_id": created["id"],
                    "status": "create",
                    "message": f"New record created with ID {created['id']}."
                })
        except Exception as e:
            error_id = record_id or f"row_{i}"
            error_message = str(e)
            logging.error(f"❌ {job_timestamp} | Record ID: {error_id} | Error: {error_message}", exc_info=True)
            log_rows.append({
                "datetime": job_timestamp,
                "record_id": error_id,
                "status": "error",
                "message": error_message
            })
    logging.info(f"Finished. Total records processed: {len(log_rows)}")
    return log_rows

def write_job_log(component, log_rows: list) -> None:
    config_id = os.getenv("KBC_CONFIGID", "unknown_config")
    destination_log = f"in.c-kds-team-app-custom-python-{config_id}.job_log"
    logging.info(f"Destination for the job log: {destination_log}")
    output_table = component.create_out_table_definition(
        "job_log.csv",
        primary_key=["record_id", "datetime"],
        destination=destination_log,
        incremental=True
    )
    output_path = output_table.full_path
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["datetime", "record_id", "status", "message"])
        writer.writeheader()
        writer.writerows(log_rows)
    component.write_manifest(output_table)
    logging.info(f"Manifest written for job log output: {output_table.destination}")
import logging
from pyairtable import Api

def fetch_airtable_field_mapping(table):
    """
    Fetch a sample record from the Airtable table to infer available field names.
    Returns a mapping of normalized field names (underscored) to actual Airtable field names,
    and a list of known computed fields that should be skipped during updates.
    """
    test_records = table.all(max_records=1)
    if not test_records:
        logging.warning("⚠️ Airtable table appears to be empty. Field mapping may not be accurate.")
        return {}, []

    airtable_fields = list(test_records[0].get("fields", {}).keys())
    logging.info(f"📋 Columns available in table '{table.name}': {airtable_fields}")

    # Hardcoded list of computed (non-editable) fields
    computed_fields = ["Total billed"]
    logging.info(f"⏭️ Computed fields (will be skipped if present in input): {computed_fields}")

    mapping = {}
    for field in airtable_fields:
        if field in computed_fields:
            continue
        normalized = field.replace(" ", "_").replace("-", "_")
        mapping[normalized] = field

    return mapping, computed_fields

def validate_airtable_connection(api_token: str, base_id: str, table_name: str):
    """
    Connects to Airtable using the API token and checks access to the base and table.
    Also retrieves the field mapping and computed field info.
    """
    try:
        api = Api(api_token)
        base = api.base(base_id)
        table = base.table(table_name)
        mapping, computed_fields = fetch_airtable_field_mapping(table)
        logging.info(f"🧭 Field mapping will be used: {mapping}")
        return table, mapping, computed_fields
    except Exception as e:
        logging.error("❌ Failed to validate Airtable credentials or access.", exc_info=True)
        raise

def normalize_column_name(col):
    """
    Normalizes column names by replacing spaces and dashes with underscores.
    Used to match input column names to Airtable fields.
    """
    return col.replace(" ", "_").replace("-", "_")
