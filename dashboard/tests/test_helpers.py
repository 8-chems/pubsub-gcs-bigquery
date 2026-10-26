import common
import helpers

GOOD = "order_id,customer_id,amount,created_at\nA-1,C-1,42.50,2026-10-25 09:15:00\n"


def test_job_id_matches_the_subscriber_rule():
    """The dashboard finds a file's load job by recomputing the subscriber's job ID."""
    uri = "gs://bucket/incoming/orders.csv"
    assert helpers.load_job_id(uri, "123") == common.load_job_id(uri, "123")
    assert helpers.load_job_id(uri, "123") != common.load_job_id(uri, "124")


def test_valid_file_has_no_problems():
    assert helpers.validate_csv(GOOD) == []


def test_empty_file_and_header_only():
    assert helpers.validate_csv("") == ["The file is empty."]
    assert "no data rows" in helpers.validate_csv("order_id,customer_id,amount,created_at\n")[0]


def test_wrong_header_is_reported():
    problems = helpers.validate_csv("id,amount\n1,2\n")
    assert len(problems) == 1 and "header must be exactly" in problems[0]


def test_bad_amount_and_timestamp_are_reported_with_line_numbers():
    text = "order_id,customer_id,amount,created_at\nA-1,C-1,abc,yesterday\n"
    problems = helpers.validate_csv(text)
    assert any("Line 2" in p and "amount" in p for p in problems)
    assert any("Line 2" in p and "created_at" in p for p in problems)


def test_wrong_column_count():
    text = "order_id,customer_id,amount,created_at\nA-1,C-1,5\n"
    assert "expected 4 columns" in helpers.validate_csv(text)[0]


def test_sample_files_behave_as_documented():
    from pathlib import Path

    samples = Path(__file__).parents[2] / "sample-data"
    assert helpers.validate_csv((samples / "orders.csv").read_text()) == []
    assert helpers.validate_csv((samples / "bad_orders.csv").read_text()) != []


def test_describe_status_covers_every_stage():
    assert helpers.describe_status(False)[0] == "waiting"
    assert helpers.describe_status(True, "RUNNING")[0] == "running"
    assert helpers.describe_status(True, "DONE", None, 2) == (
        "ok",
        "Loaded 2 row(s) into BigQuery.",
    )
    level, text = helpers.describe_status(True, "DONE", "Could not parse 'x' as FLOAT")
    assert level == "error" and "dead-letter" in text
