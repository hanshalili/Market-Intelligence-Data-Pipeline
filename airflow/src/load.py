"""
load.py — Load curated Parquet data from GCS into BigQuery.

Design decisions:
- Uses WRITE_TRUNCATE per partition so the load is idempotent:
  re-running for the same date always produces the same result.
- Loads from the GCS Parquet URI directly — no local I/O required.
- Relies on the table schema defined in Terraform; this module does
  not create or alter tables.
- Adds an `ingested_at` timestamp column at load time for auditability.
"""

import logging
from datetime import date, datetime, timezone

import pandas as pd
from google.cloud import bigquery, storage

from src.config import config

logger = logging.getLogger(__name__)


def load_parquet_to_bigquery(execution_date: date) -> int:
    """
    Load the curated Parquet file for execution_date from GCS into BigQuery.

    The function:
    1. Downloads the Parquet from GCS into memory.
    2. Adds the `ingested_at` audit column.
    3. Deduplicates rows by (date, symbol) — last row wins.
    4. Uploads to the staging table with WRITE_TRUNCATE on the date partition.

    Args:
        execution_date: The trading date whose Parquet file to load.

    Returns:
        Number of rows inserted.

    Raises:
        FileNotFoundError: If the curated Parquet does not exist in GCS.
        google.api_core.exceptions.GoogleAPIError: On BigQuery load failures.
    """
    gcs_client = storage.Client()
    bq_client = bigquery.Client(project=config.gcp_project_id)

    gcs_path = f"curated/stock_prices/date={execution_date.isoformat()}/data.parquet"
    bucket = gcs_client.bucket(config.gcs_bucket_name)
    blob = bucket.blob(gcs_path)

    if not blob.exists():
        raise FileNotFoundError(
            f"Curated Parquet not found: gs://{config.gcs_bucket_name}/{gcs_path}. "
            "Did the transform step run successfully?"
        )

    logger.info("Downloading Parquet from gs://%s/%s", config.gcs_bucket_name, gcs_path)
    parquet_bytes = blob.download_as_bytes()

    import io
    df = pd.read_parquet(io.BytesIO(parquet_bytes))

    if df.empty:
        logger.warning("Parquet file for %s is empty — skipping BigQuery load.", execution_date)
        return 0

    # Add audit timestamp
    df["ingested_at"] = datetime.now(tz=timezone.utc)

    # Deduplicate: keep latest row per (date, symbol) in case of re-runs
    df = df.sort_values("ingested_at").drop_duplicates(
        subset=["date", "symbol"], keep="last"
    )

    row_count = len(df)
    logger.info("Preparing to load %d rows into BigQuery for date=%s", row_count, execution_date)

    # Append the partition decorator ($YYYYMMDD) so WRITE_TRUNCATE replaces only
    # this date's partition, not the entire table.
    partition_suffix = execution_date.strftime("%Y%m%d")
    table_ref = (
        f"{config.gcp_project_id}.{config.bq_dataset}.{config.bq_staging_table}"
        f"${partition_suffix}"
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema_update_options=[],
        autodetect=False,
        schema=_build_bq_schema(),
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        ),
    )

    logger.info("Loading %d rows into table: %s", len(df), table_ref)

    load_job = bq_client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config,
    )
    load_job.result()  # Blocks until complete; raises on failure

    logger.info(
        "BigQuery load complete: %d rows → %s (partition %s)",
        row_count,
        table_ref,
        execution_date,
    )
    return row_count


def verify_load(execution_date: date) -> int:
    """
    Query BigQuery to confirm the row count matches what was loaded.

    Args:
        execution_date: The partition to verify.

    Returns:
        Row count in BigQuery for this date.
    """
    bq_client = bigquery.Client(project=config.gcp_project_id)

    query = f"""
        SELECT COUNT(*) AS row_count
        FROM `{config.gcp_project_id}.{config.bq_dataset}.{config.bq_staging_table}`
        WHERE date = '{execution_date.isoformat()}'
    """

    result = bq_client.query(query).result()
    row_count = next(result)["row_count"]

    logger.info("BigQuery verification: %d rows found for date=%s", row_count, execution_date)
    return row_count


# ------------------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------------------

def _build_bq_schema():
    """Return the BigQuery schema matching stg_stock_prices."""
    return [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("open", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("high", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("low", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("close", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("adjusted_close", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("volume", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    ]
