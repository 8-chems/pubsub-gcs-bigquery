"""Shared processing logic used by both the pull subscriber and the push endpoint."""

import hashlib
import logging
import os

from google.api_core.exceptions import Conflict
from google.cloud import bigquery

PROJECT_ID = os.environ["GCP_PROJECT"]
DATASET = os.environ.get("BQ_DATASET", "raw")
TABLE = os.environ.get("BQ_TABLE", "orders")
LOCATION = os.environ.get("BQ_LOCATION", "EU")
TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE}"

SCHEMA = [
    bigquery.SchemaField("order_id", "STRING"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("amount", "FLOAT"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
]

_bq = None


def load_job_id(uri: str, generation: str) -> str:
    """Deterministic BigQuery job ID for one upload (object URI + generation).

    The Streamlit dashboard mirrors this rule to find the job of a file it uploaded;
    a test keeps the two copies identical.
    """
    return "load_" + hashlib.sha256(f"{uri}#{generation}".encode()).hexdigest()[:40]


def _client() -> bigquery.Client:
    """Create the BigQuery client on first use, so importing this module needs no credentials."""
    global _bq
    if _bq is None:
        _bq = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    return _bq


def handle_event(attrs: dict) -> str:
    """Process one GCS notification. Raise on failure so the caller nacks/retries.

    GCS notifications carry the useful fields as message *attributes*:
    eventType, bucketId, objectId, objectGeneration.
    """
    if attrs.get("eventType") != "OBJECT_FINALIZE":
        return "ignored: not a finalize event"

    bucket, name = attrs["bucketId"], attrs["objectId"]
    generation = attrs.get("objectGeneration", "0")
    if not name.endswith(".csv"):
        return f"ignored: {name} is not a CSV"

    uri = f"gs://{bucket}/{name}"

    # Deterministic job id => idempotent. Pub/Sub is at-least-once, so the same
    # message can arrive twice; BigQuery refuses to create the same job id twice.
    job_id = load_job_id(uri, generation)

    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        max_bad_records=0,
    )

    client = _client()
    try:
        job = client.load_table_from_uri(uri, TABLE_ID, job_id=job_id, job_config=config)
    except Conflict:
        logging.info("Job %s already exists, waiting on it", job_id)
        job = client.get_job(job_id)

    job.result()  # raises if the load failed -> message is retried, then dead-lettered
    return f"loaded {uri} (job {job_id})"
