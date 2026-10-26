"""Run the Streamlit script headlessly with Streamlit's AppTest. No real GCP calls are made."""

import datetime
from pathlib import Path

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

APP = str(Path(__file__).parents[1] / "streamlit_app.py")


class FakeJob:
    state = "DONE"
    error_result = None
    output_rows = 2


class FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class FakeBigQuery:
    def __init__(self, *args, **kwargs):
        pass

    def get_job(self, job_id):
        return FakeJob()

    def query(self, sql):
        if "AS rows_loaded" in sql:
            return FakeQuery([{"rows_loaded": 2, "revenue": 60.5, "customers": 2}])
        if "GROUP BY day" in sql:
            return FakeQuery([{"day": datetime.date(2026, 10, 25), "orders": 2}])
        return FakeQuery([{"order_id": "A-1001", "amount": 42.5}])


class FakeBlob:
    generation = 1700000000000001

    def __init__(self, name, sink):
        self.name, self._sink = name, sink

    def upload_from_string(self, data, content_type=None):
        self._sink.append((self.name, data))


class FakeStorage:
    uploads: list = []

    def __init__(self, *args, **kwargs):
        pass

    def bucket(self, name):
        sink = FakeStorage.uploads
        return type("B", (), {"blob": lambda _self, n: FakeBlob(n, sink)})()

    def list_blobs(self, *args, **kwargs):
        return []


class FakeSubscriber:
    def __init__(self, *args, **kwargs):
        pass

    def pull(self, request, timeout=None):
        return type("R", (), {"received_messages": []})()


class FakeMonitoring:
    def __init__(self, *args, **kwargs):
        pass

    def list_time_series(self, request):
        return []


@pytest.fixture
def fake_gcp(monkeypatch):
    import streamlit as st
    from google.cloud import bigquery, monitoring_v3, pubsub_v1, storage

    st.cache_resource.clear()
    st.cache_data.clear()
    FakeStorage.uploads = []
    monkeypatch.setattr(bigquery, "Client", FakeBigQuery)
    monkeypatch.setattr(storage, "Client", FakeStorage)
    monkeypatch.setattr(pubsub_v1, "SubscriberClient", FakeSubscriber)
    monkeypatch.setattr(monitoring_v3, "MetricServiceClient", FakeMonitoring)
    monkeypatch.setenv("GCP_PROJECT", "demo")


def test_asks_for_a_project_when_none_is_set(monkeypatch):
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert not at.exception
    assert any("project ID" in i.value for i in at.info)


def test_reports_missing_credentials_instead_of_crashing(monkeypatch):
    import streamlit as st

    st.cache_resource.clear()
    monkeypatch.setenv("GCP_PROJECT", "demo")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent/creds.json")
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert not at.exception
    assert any("Could not create Google Cloud clients" in e.value for e in at.error)


def test_uploading_a_sample_and_tracking_it(fake_gcp):
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert not at.exception

    at.selectbox[0].select("orders.csv (valid)").run()
    assert any("matches the table schema" in s.value for s in at.success)

    send = next(b for b in at.button if b.label == "Send to pipeline")
    send.click().run()
    assert not at.exception
    assert len(FakeStorage.uploads) == 1
    object_name, data = FakeStorage.uploads[0]
    assert object_name.startswith("incoming/orders-") and object_name.endswith(".csv")
    assert b"A-1001" in data

    tracked = at.session_state["tracked"]
    assert tracked[0]["uri"] == f"gs://demo-landing/{object_name}"
    assert any("Loaded 2 row(s)" in s.value for s in at.success)


def test_invalid_file_needs_explicit_confirmation(fake_gcp):
    at = AppTest.from_file(APP, default_timeout=30).run()
    at.selectbox[0].select("bad_orders.csv (will fail: tests the dead-letter path)").run()
    assert any("will probably fail" in w.value for w in at.warning)
    send = next(b for b in at.button if b.label == "Send to pipeline")
    assert send.disabled
