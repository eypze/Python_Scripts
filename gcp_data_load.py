"""
gcp_data_load.py
Creates an empty table in Google BigQuery based on a CSV file that defines the schema.

Expected CSV structure:
  Table Name, Field Name, KEYFLAG, Type, Len, Dec, Description

Supported data types (column 'Type') and their BigQuery mapping:
  CHAR, VARCHAR, STRING  -> STRING
  INT, INTEGER           -> INTEGER
  FLOAT                  -> FLOAT
  DECIMAL, NUMERIC       -> NUMERIC  (uses Len and Dec for precision info)
  DATE                   -> DATE
  DATETIME, TIMESTAMP    -> TIMESTAMP
  BOOLEAN, BOOL          -> BOOLEAN
  BYTES                  -> BYTES
  (any other)            -> STRING   (default fallback)

Requirements:
  pip install google-cloud-bigquery pandas

Authentication:
  Set the GOOGLE_APPLICATION_CREDENTIALS environment variable pointing to your
  service account key file (.json), or use Application Default Credentials
  (gcloud auth application-default login).
"""

import os
import pandas as pd
from google.cloud import bigquery


# ---------------------------------------------------------------------------
# Mapping of CSV data types to BigQuery data types
# ---------------------------------------------------------------------------
TYPE_MAP = {
    "CHAR":      "STRING",
    "VARCHAR":   "STRING",
    "STRING":    "STRING",
    "TEXT":      "STRING",
    "INT":       "INTEGER",
    "INTEGER":   "INTEGER",
    "INT64":     "INTEGER",
    "FLOAT":     "FLOAT",
    "FLOAT64":   "FLOAT",
    "DECIMAL":   "NUMERIC",
    "NUMERIC":   "NUMERIC",
    "DATE":      "DATE",
    "DATETIME":  "DATETIME",
    "TIMESTAMP": "TIMESTAMP",
    "BOOLEAN":   "BOOLEAN",
    "BOOL":      "BOOLEAN",
    "BYTES":     "BYTES",
}


def map_type(csv_type: str) -> str:
    """Converts a CSV type string to the equivalent BigQuery type."""
    return TYPE_MAP.get(csv_type.strip().upper(), "STRING")


def build_schema_from_csv(csv_file_path: str) -> tuple[str, list[bigquery.SchemaField]]:
    """
    Reads the CSV file using pandas and builds the table name and BigQuery schema.

    Args:
        csv_file_path: Path to the CSV file containing the schema definition.

    Returns:
        A tuple (table_name, schema) where schema is a list of SchemaField objects.
    """
    # Read CSV with pandas — auto-detects delimiter (comma, semicolon, tab, etc.)
    df = pd.read_csv(csv_file_path, dtype=str, encoding="utf-8-sig", sep=None, engine="python")

    # Strip whitespace from column names and all string values
    df.columns = df.columns.str.strip()
    df = df.apply(lambda col: col.str.strip() if col.dtype == object else col)

    # Drop rows where 'Field Name' is missing (empty or NaN)
    df = df.dropna(subset=["Field Name"])
    df = df[df["Field Name"] != ""]

    if df.empty:
        raise ValueError("No fields were found in the CSV file.")

    # Read the table name from the first row
    table_name = df["Table Name"].iloc[0] if "Table Name" in df.columns else ""
    if not table_name:
        raise ValueError("'Table Name' column not found or empty in the CSV file.")

    schema = []
    seen_fields = set()  # Track field names to avoid duplicates
    duplicates = []

    for _, row in df.iterrows():
        field_name  = row.get("Field Name", "")
        keyflag     = str(row.get("KEYFLAG", "")).upper()
        field_type  = row.get("Type", "STRING") or "STRING"
        length      = str(row.get("Len", "") or "")
        decimals    = str(row.get("Dec", "") or "")
        description = str(row.get("Description", "") or "")

        # Skip duplicate field names — BigQuery does not allow them
        if field_name in seen_fields:
            duplicates.append(field_name)
            continue
        seen_fields.add(field_name)

        bq_type = map_type(field_type)

        # For NUMERIC/DECIMAL fields, append precision info to the description
        # since BigQuery SchemaField does not accept precision/scale directly
        if bq_type == "NUMERIC" and (length or decimals):
            prec_info = f" (Len={length}, Dec={decimals})"
            description = description + prec_info if description else prec_info.strip()

        # REQUIRED mode for key fields, NULLABLE for all others
        mode = "REQUIRED" if keyflag in ("TRUE", "1", "YES", "X") else "NULLABLE"

        field = bigquery.SchemaField(
            name=field_name,
            field_type=bq_type,
            mode=mode,
            description=description if description else None,
        )
        schema.append(field)

    if duplicates:
        print(f"  Warning: {len(duplicates)} duplicate field(s) skipped: {', '.join(duplicates)}")

    return table_name, schema


def create_table_in_bigquery(
    project_id: str,
    dataset_id: str,
    table_name: str,
    schema: list[bigquery.SchemaField],
    exists_ok: bool = True,
) -> None:
    """
    Creates an empty table in BigQuery with the provided schema.

    Args:
        project_id:  GCP project ID.
        dataset_id:  BigQuery dataset ID.
        table_name:  Name of the table to create.
        schema:      List of SchemaField objects defining the table columns.
        exists_ok:   If True, no error is raised if the table already exists.
    """
    client = bigquery.Client(project=project_id)

    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    table = bigquery.Table(table_ref, schema=schema)

    table = client.create_table(table, exists_ok=exists_ok)
    print(f"Table created successfully: {table.full_table_id}")
    print(f"  Dataset : {dataset_id}")
    print(f"  Table   : {table_name}")
    print(f"  Fields  : {len(schema)}")


# ---------------------------------------------------------------------------
# Main block — update the variables below to match your environment
# ---------------------------------------------------------------------------
if __name__ == "__main__":

    # ── Configuration ──────────────────────────────────────────────────────
    PROJECT_ID       = r"ey-playground"   # GCP project ID
    DATASET_ID       = r"Playground"      # BigQuery dataset ID
    CSV_PATH         = r"D:\Users\eypze\Documents\Project Very Fast Data\selection.csv"  # Path to the CSV schema file
    CREDENTIALS_PATH = r"D:\Users\eypze\Documents\Project Very Fast Data\ey-playground-47b7a406cfbd.json"  # Path to GCP service account key file
    # ───────────────────────────────────────────────────────────────────────

    # Set GCP credentials via environment variable (Option 1: service account key file)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
    print(f"Using credentials: {CREDENTIALS_PATH}")

    print(f"Reading schema from: {CSV_PATH}")
    table_name, schema = build_schema_from_csv(CSV_PATH)

    print(f"Table name detected : {table_name}")
    print(f"Fields detected     : {len(schema)}")
    for field in schema:
        print(f"  - {field.name} ({field.field_type}, {field.mode})"
              + (f" | {field.description}" if field.description else ""))

    print("\nCreating table in BigQuery...")
    create_table_in_bigquery(PROJECT_ID, DATASET_ID, table_name, schema)
