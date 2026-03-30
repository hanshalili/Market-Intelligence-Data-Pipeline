"""
daily_market_pipeline.py — Airflow DAG: Apple & Tesla Market Intelligence Pipeline

Schedule: Runs at 21:00 UTC (4 PM US Eastern, 30 min after NYSE close)
          every weekday. The DAG skips weekends and market holidays gracefully.

Pipeline steps (in order):
  1. extract_alpha_vantage   — Pull OHLCV data from Alpha Vantage API
  2. store_raw_to_gcs        — Upload raw JSON to GCS raw/ layer
  3. transform_to_parquet    — Normalise & convert to Parquet
  4. upload_parquet_to_gcs   — Upload curated Parquet to GCS curated/ layer
  5. load_to_bigquery        — Load Parquet into BigQuery staging table
  6. verify_bq_load          — Assert row count > 0
  7. dbt_run                 — Run dbt transformations (mart_daily_metrics)
  8. dbt_test                — Run dbt data quality tests

Design choices:
- Each task is a single-responsibility Python function.
- XComs pass lightweight metadata (file paths, row counts) between tasks.
- The DAG is idempotent: re-running for the same date overwrites cleanly.
- Empty-result days (holidays) are handled without failing the pipeline.
"""

import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# ---------------------------------------------------------------------------
# DAG default arguments
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="daily_market_pipeline",
    description="Daily batch pipeline: Alpha Vantage → GCS → BigQuery → dbt",
    default_args=DEFAULT_ARGS,
    # Run at 21:00 UTC Monday-Friday (after NYSE close at 20:30 UTC)
    schedule_interval="0 21 * * 1-5",
    start_date=days_ago(1),
    catchup=False,  # Do not backfill missed runs automatically
    max_active_runs=1,  # Prevent concurrent pipeline runs
    tags=["market", "daily", "batch"],
    doc_md="""
## Daily Market Intelligence Pipeline

Ingests AAPL, TSLA, and SPY end-of-day data from Alpha Vantage,
stores it in GCS, loads it into BigQuery, and runs dbt transformations
to produce the `mart_daily_metrics` analytics table.

### Data flow
```
Alpha Vantage API
    ↓ (JSON)
GCS raw/{symbol}/date=YYYY-MM-DD/data.json
    ↓ (parse + Parquet)
GCS curated/stock_prices/date=YYYY-MM-DD/data.parquet
    ↓ (BigQuery Load Job)
stg_stock_prices (partitioned by date, clustered by symbol)
    ↓ (dbt)
mart_daily_metrics
```
    """,
) as dag:

    # -----------------------------------------------------------------------
    # TASK 1: Extract — pull raw data from Alpha Vantage for all symbols
    # -----------------------------------------------------------------------
    def _extract_alpha_vantage(**context) -> str:
        """
        Fetch daily adjusted data for AAPL, TSLA, SPY.

        Stores each symbol's raw API response on the local container filesystem
        so it can be uploaded to GCS in the next step.

        Returns: JSON string mapping symbol → local file path.
        """
        import sys

        sys.path.insert(0, "/opt/airflow")

        from src.config import config
        from src.extract import fetch_daily_adjusted
        import json
        import os
        import time

        execution_date = context["ds"]  # YYYY-MM-DD string
        logger = logging.getLogger(__name__)
        logger.info("Starting extract for execution_date=%s", execution_date)

        raw_dir = os.path.join(config.local_data_dir, "raw")
        os.makedirs(raw_dir, exist_ok=True)

        symbol_paths = {}

        for i, symbol in enumerate(config.symbols):
            if i > 0:
                logger.info("Sleeping %ss for API rate limit", config.api_sleep_seconds)
                time.sleep(config.api_sleep_seconds)

            # Fetch full compact history (last 100 days)
            df = fetch_daily_adjusted(symbol, output_size="compact")

            if df.empty:
                logger.warning(
                    "No data returned for symbol=%s on %s", symbol, execution_date
                )
                continue

            # Save as JSON lines so downstream can reconstruct the DataFrame
            local_path = os.path.join(
                raw_dir, f"{symbol.lower()}_{execution_date}.json"
            )
            df.to_json(local_path, orient="records", date_format="iso")
            symbol_paths[symbol] = local_path
            logger.info("Saved %d rows for %s to %s", len(df), symbol, local_path)

        if not symbol_paths:
            logger.warning(
                "No data extracted for any symbol on %s. "
                "This is expected on market holidays.",
                execution_date,
            )

        return json.dumps(symbol_paths)

    extract_task = PythonOperator(
        task_id="extract_alpha_vantage",
        python_callable=_extract_alpha_vantage,
        doc_md="Pull daily adjusted OHLCV data for AAPL, TSLA, SPY from Alpha Vantage.",
    )

    # -----------------------------------------------------------------------
    # TASK 2: Store raw JSON to GCS
    # -----------------------------------------------------------------------
    def _store_raw_to_gcs(**context) -> str:
        """
        Upload each symbol's raw JSON file to the GCS raw/ layer.

        Returns: JSON string mapping symbol → GCS URI.
        """
        import json
        import sys

        sys.path.insert(0, "/opt/airflow")
        from src.config import config
        from google.cloud import storage

        logger = logging.getLogger(__name__)
        execution_date = context["ds"]

        # Read XCom from extract task
        ti = context["ti"]
        symbol_paths_json = ti.xcom_pull(task_ids="extract_alpha_vantage")
        symbol_paths = json.loads(symbol_paths_json)

        if not symbol_paths:
            logger.info("No symbol paths to upload (likely a market holiday).")
            return json.dumps({})

        gcs_client = storage.Client()
        bucket = gcs_client.bucket(config.gcs_bucket_name)
        gcs_uris = {}

        for symbol, local_path in symbol_paths.items():
            gcs_path = f"raw/{symbol.lower()}/date={execution_date}/data.json"
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path, content_type="application/json")
            gcs_uri = f"gs://{config.gcs_bucket_name}/{gcs_path}"
            gcs_uris[symbol] = gcs_uri
            logger.info("Uploaded %s → %s", local_path, gcs_uri)

        return json.dumps(gcs_uris)

    store_raw_task = PythonOperator(
        task_id="store_raw_to_gcs",
        python_callable=_store_raw_to_gcs,
        doc_md="Upload raw Alpha Vantage JSON responses to GCS raw/ layer.",
    )

    # -----------------------------------------------------------------------
    # TASK 3: Transform raw JSON → Parquet
    # -----------------------------------------------------------------------
    def _transform_to_parquet(**context) -> str:
        """
        Read each symbol's raw JSON, combine into a single DataFrame,
        and write as a Parquet file partitioned by date.

        Returns: local path to the Parquet file.
        """
        import json
        import sys

        sys.path.insert(0, "/opt/airflow")
        from src.transform import dataframe_to_parquet_local
        from datetime import date as date_cls
        import pandas as pd

        logger = logging.getLogger(__name__)
        execution_date_str = context["ds"]

        ti = context["ti"]
        symbol_paths_json = ti.xcom_pull(task_ids="extract_alpha_vantage")
        symbol_paths = json.loads(symbol_paths_json)

        if not symbol_paths:
            logger.info("No data to transform — skipping Parquet creation.")
            return ""

        frames = []
        for symbol, local_path in symbol_paths.items():
            df = pd.read_json(local_path, orient="records")
            df["date"] = pd.to_datetime(df["date"]).dt.date
            frames.append(df)
            logger.info("Added %d rows for %s (all available dates)", len(df), symbol)

        if not frames:
            logger.warning("No data to transform.")
            return ""

        combined = pd.concat(frames, ignore_index=True)
        exec_date = date_cls.fromisoformat(execution_date_str)
        local_path = dataframe_to_parquet_local(combined, exec_date)
        logger.info("Parquet written: %s", local_path)
        return local_path

    transform_task = PythonOperator(
        task_id="transform_to_parquet",
        python_callable=_transform_to_parquet,
        doc_md="Normalise raw JSON data and write to Parquet (Snappy compressed).",
    )

    # -----------------------------------------------------------------------
    # TASK 4: Upload curated Parquet to GCS
    # -----------------------------------------------------------------------
    def _upload_parquet_to_gcs(**context) -> str:
        """Upload the curated Parquet file to GCS curated/ layer."""
        import sys

        sys.path.insert(0, "/opt/airflow")
        from src.transform import upload_parquet_to_gcs
        from datetime import date as date_cls

        logger = logging.getLogger(__name__)
        execution_date_str = context["ds"]

        ti = context["ti"]
        local_path = ti.xcom_pull(task_ids="transform_to_parquet")

        if not local_path:
            logger.info("No Parquet file to upload — skipping.")
            return ""

        exec_date = date_cls.fromisoformat(execution_date_str)
        gcs_uri = upload_parquet_to_gcs(local_path, exec_date)
        return gcs_uri

    upload_parquet_task = PythonOperator(
        task_id="upload_parquet_to_gcs",
        python_callable=_upload_parquet_to_gcs,
        doc_md="Upload curated Parquet to gs://bucket/curated/stock_prices/date=YYYY-MM-DD/",
    )

    # -----------------------------------------------------------------------
    # TASK 5: Load curated Parquet from GCS into BigQuery
    # -----------------------------------------------------------------------
    def _load_to_bigquery(**context) -> int:
        """Load curated Parquet into BigQuery stg_stock_prices."""
        import sys

        sys.path.insert(0, "/opt/airflow")
        from src.load import load_parquet_to_bigquery
        from datetime import date as date_cls

        logger = logging.getLogger(__name__)
        execution_date_str = context["ds"]

        ti = context["ti"]
        gcs_uri = ti.xcom_pull(task_ids="upload_parquet_to_gcs")

        if not gcs_uri:
            logger.info("No GCS URI available — skipping BigQuery load.")
            return 0

        exec_date = date_cls.fromisoformat(execution_date_str)
        rows_loaded = load_parquet_to_bigquery(exec_date)
        logger.info(
            "Loaded %d rows into BigQuery for %s", rows_loaded, execution_date_str
        )
        return rows_loaded

    load_bq_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=_load_to_bigquery,
        doc_md="Load curated Parquet from GCS into BigQuery (WRITE_TRUNCATE on date partition).",
    )

    # -----------------------------------------------------------------------
    # TASK 6: Verify BigQuery load
    # -----------------------------------------------------------------------
    def _verify_bq_load(**context):
        """Assert that at least some rows were loaded for the execution date."""
        import sys

        sys.path.insert(0, "/opt/airflow")
        from src.load import verify_load
        from datetime import date as date_cls

        logger = logging.getLogger(__name__)
        execution_date_str = context["ds"]

        ti = context["ti"]
        rows_loaded = ti.xcom_pull(task_ids="load_to_bigquery")

        if rows_loaded == 0:
            logger.warning(
                "0 rows loaded for %s — skipping verification (likely a holiday).",
                execution_date_str,
            )
            return

        exec_date = date_cls.fromisoformat(execution_date_str)
        bq_count = verify_load(exec_date)

        assert bq_count > 0, (
            f"BigQuery verification FAILED: 0 rows found for date={execution_date_str}. "
            "Check the load job logs."
        )
        logger.info(
            "Verification PASSED: %d rows in BigQuery for %s",
            bq_count,
            execution_date_str,
        )

    verify_task = PythonOperator(
        task_id="verify_bq_load",
        python_callable=_verify_bq_load,
        doc_md="Assert that BigQuery contains > 0 rows for the execution date.",
    )

    # -----------------------------------------------------------------------
    # TASK 7: dbt run — materialise mart_daily_metrics
    # -----------------------------------------------------------------------
    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt deps --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt && "
            "dbt run "
            "--profiles-dir /opt/airflow/dbt "
            "--project-dir /opt/airflow/dbt "
            "--select +mart_daily_metrics "
            '--vars \'{"execution_date": "{{ ds }}"}\' '
            "--full-refresh "
            "--no-version-check"
        ),
        doc_md="Run dbt to compute SMA, returns, volatility, drawdown, and excess return.",
    )

    # -----------------------------------------------------------------------
    # TASK 8: dbt test — run data quality tests
    # -----------------------------------------------------------------------
    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test "
            "--profiles-dir /opt/airflow/dbt "
            "--project-dir /opt/airflow/dbt "
            "--select +mart_daily_metrics "
            "--no-version-check"
        ),
        doc_md="Run dbt tests: unique(symbol, date), not_null constraints.",
    )

    # -----------------------------------------------------------------------
    # Task dependencies — linear pipeline
    # -----------------------------------------------------------------------
    (
        extract_task
        >> store_raw_task
        >> transform_task
        >> upload_parquet_task
        >> load_bq_task
        >> verify_task
        >> dbt_run_task
        >> dbt_test_task
    )
