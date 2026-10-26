"""Pure helpers for the Streamlit dashboard.

No Streamlit and no Google Cloud imports here, so everything is trivial to unit test.
"""

import csv
import hashlib
import io
from datetime import datetime

EXPECTED_COLUMNS = ["order_id", "customer_id", "amount", "created_at"]
MAX_PROBLEMS = 8


def load_job_id(uri: str, generation: str) -> str:
    """Same rule as app/common.py: one BigQuery job per (object URI, generation)."""
    return "load_" + hashlib.sha256(f"{uri}#{generation}".encode()).hexdigest()[:40]


def _is_timestamp(value: str) -> bool:
    try:
        datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def validate_csv(text: str) -> list[str]:
    """Return human-readable problems. An empty list means BigQuery should accept the file."""
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        return ["The file is empty."]

    if [h.strip() for h in header] != EXPECTED_COLUMNS:
        return [
            f"The header must be exactly: {', '.join(EXPECTED_COLUMNS)}. "
            f"Found: {', '.join(header)}."
        ]

    problems: list[str] = []
    data_rows = 0
    for line_no, row in enumerate(reader, start=2):
        if not row:
            continue
        data_rows += 1
        if len(row) != len(EXPECTED_COLUMNS):
            problems.append(f"Line {line_no}: expected 4 columns, found {len(row)}.")
        else:
            amount, created_at = row[2], row[3]
            try:
                float(amount)
            except ValueError:
                problems.append(f"Line {line_no}: amount {amount!r} is not a number.")
            if not _is_timestamp(created_at):
                problems.append(f"Line {line_no}: created_at {created_at!r} is not a timestamp.")
        if len(problems) >= MAX_PROBLEMS:
            problems.append("...and possibly more.")
            break

    if data_rows == 0:
        problems.append("The file has a header but no data rows.")
    return problems


def describe_status(
    found: bool,
    state: str | None = None,
    error: str | None = None,
    rows: int | None = None,
) -> tuple[str, str]:
    """Turn what we know about a load job into (level, message).

    level is one of: waiting, running, ok, error.
    """
    if not found:
        return "waiting", "In Cloud Storage. Waiting for the subscriber to create a load job."
    if error:
        return (
            "error",
            f"BigQuery rejected the load: {error} Pub/Sub will retry, then move the message "
            "to the dead-letter topic.",
        )
    if state != "DONE":
        return "running", f"Load job is {(state or 'pending').lower()}."
    if rows is None:
        return "ok", "Loaded into BigQuery."
    return "ok", f"Loaded {rows} row(s) into BigQuery."
