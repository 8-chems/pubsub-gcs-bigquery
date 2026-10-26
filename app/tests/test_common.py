from unittest.mock import MagicMock

import common
import pytest
from google.api_core.exceptions import Conflict


def finalize(name="incoming/orders.csv", generation="123", event="OBJECT_FINALIZE"):
    return {
        "eventType": event,
        "bucketId": "b",
        "objectId": name,
        "objectGeneration": generation,
    }


@pytest.fixture
def bq(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(common, "_client", lambda: client)
    return client


def test_ignores_other_event_types(bq):
    assert common.handle_event(finalize(event="OBJECT_DELETE")).startswith("ignored")
    bq.load_table_from_uri.assert_not_called()


def test_ignores_non_csv_files(bq):
    assert common.handle_event(finalize(name="incoming/readme.txt")).startswith("ignored")
    bq.load_table_from_uri.assert_not_called()


def test_loads_csv_straight_from_gcs(bq):
    result = common.handle_event(finalize())

    args, kwargs = bq.load_table_from_uri.call_args
    assert args == ("gs://b/incoming/orders.csv", common.TABLE_ID)
    assert kwargs["job_id"].startswith("load_")
    assert kwargs["job_config"].skip_leading_rows == 1
    assert kwargs["job_config"].write_disposition == "WRITE_APPEND"
    bq.load_table_from_uri.return_value.result.assert_called_once()
    assert result.startswith("loaded gs://b/incoming/orders.csv")


def test_job_id_is_deterministic_and_generation_sensitive(bq):
    def job_id_for(generation):
        bq.load_table_from_uri.reset_mock()
        common.handle_event(finalize(generation=generation))
        return bq.load_table_from_uri.call_args.kwargs["job_id"]

    assert job_id_for("1") == job_id_for("1")  # redelivery -> same job id
    assert job_id_for("1") != job_id_for("2")  # new upload -> new job id


def test_duplicate_delivery_waits_on_existing_job(bq):
    bq.load_table_from_uri.side_effect = Conflict("job exists")

    common.handle_event(finalize())

    job_id = bq.load_table_from_uri.call_args.kwargs["job_id"]
    bq.get_job.assert_called_once_with(job_id)
    bq.get_job.return_value.result.assert_called_once()


def test_failed_load_raises_so_message_is_retried(bq):
    bq.load_table_from_uri.return_value.result.side_effect = RuntimeError("bad row")
    with pytest.raises(RuntimeError):
        common.handle_event(finalize())
