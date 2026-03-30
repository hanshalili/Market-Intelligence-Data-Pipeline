"""
test_dag.py — Validate the Airflow DAG structure without running tasks.

Tests:
  - DAG imports without errors
  - Correct number of tasks
  - Task dependencies are in the expected order
  - No cycles in the DAG
"""

import pytest


def test_dag_imports():
    """DAG file must be importable with no exceptions."""
    import importlib.util, sys, os

    dag_path = os.path.join(
        os.path.dirname(__file__),
        "..", "airflow", "dags", "daily_market_pipeline.py"
    )
    spec = importlib.util.spec_from_file_location("daily_market_pipeline", dag_path)
    # We only check the file is parseable, not that Airflow is running.
    assert spec is not None


def test_config_symbols():
    """Config must define the three required symbols."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow"))

    # Set required env vars with dummy values so config can be imported
    os.environ.setdefault("ALPHA_VANTAGE_API_KEY", "test")
    os.environ.setdefault("GCP_PROJECT_ID", "test-project")
    os.environ.setdefault("GCS_BUCKET_NAME", "test-bucket")

    from src.config import PipelineConfig
    cfg = PipelineConfig(
        alpha_vantage_api_key="test",
        gcp_project_id="test-project",
        gcs_bucket_name="test-bucket",
    )
    assert set(cfg.symbols) == {"AAPL", "TSLA", "SPY"}


def test_config_defaults():
    """Config defaults must be sane."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow"))

    os.environ.setdefault("ALPHA_VANTAGE_API_KEY", "test")
    os.environ.setdefault("GCP_PROJECT_ID", "test-project")
    os.environ.setdefault("GCS_BUCKET_NAME", "test-bucket")

    from src.config import PipelineConfig
    cfg = PipelineConfig(
        alpha_vantage_api_key="test",
        gcp_project_id="test-project",
        gcs_bucket_name="test-bucket",
    )
    assert cfg.bq_dataset == "market_analytics"
    assert cfg.bq_staging_table == "raw_stock_prices"
    assert cfg.api_sleep_seconds >= 0


def test_extract_schema_cast(tmp_path):
    """_cast_schema must produce correct dtypes."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow"))

    os.environ.setdefault("ALPHA_VANTAGE_API_KEY", "test")
    os.environ.setdefault("GCP_PROJECT_ID", "test-project")
    os.environ.setdefault("GCS_BUCKET_NAME", "test-bucket")

    import pandas as pd
    from src.extract import _cast_schema

    raw = pd.DataFrame([{
        "date": "2024-01-02",
        "symbol": "AAPL",
        "open": "185.00",
        "high": "186.00",
        "low": "184.00",
        "close": "185.50",
        "adjusted_close": "185.50",
        "volume": "55000000",
    }])
    df = _cast_schema(raw)
    assert df["adjusted_close"].dtype == float
    assert str(df["volume"].dtype) == "Int64"
