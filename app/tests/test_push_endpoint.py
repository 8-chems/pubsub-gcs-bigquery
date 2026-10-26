import main
import pytest


@pytest.fixture
def client():
    return main.app.test_client()


def envelope(attrs=None):
    return {"message": {"messageId": "1", "attributes": attrs or {}}, "subscription": "s"}


def test_rejects_invalid_envelope(client):
    assert client.post("/", json={}).status_code == 400


def test_acks_with_204_on_success(client, monkeypatch):
    monkeypatch.setattr(main, "handle_event", lambda attrs: "ok")
    assert client.post("/", json=envelope()).status_code == 204


def test_returns_500_on_failure_so_pubsub_retries(client, monkeypatch):
    def boom(attrs):
        raise RuntimeError("load failed")

    monkeypatch.setattr(main, "handle_event", boom)
    assert client.post("/", json=envelope()).status_code == 500
