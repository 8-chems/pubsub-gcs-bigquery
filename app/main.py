"""Push endpoint for Cloud Run. Pub/Sub POSTs each message here as JSON.

Any non-2xx response counts as a nack, so Pub/Sub retries with backoff.
"""

import logging

from common import handle_event
from flask import Flask, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)


@app.post("/")
def receive():
    envelope = request.get_json(silent=True)
    if not envelope or "message" not in envelope:
        return "invalid Pub/Sub envelope", 400

    msg = envelope["message"]
    try:
        result = handle_event(msg.get("attributes", {}))
    except Exception:
        logging.exception("message %s failed", msg.get("messageId"))
        return "processing error", 500  # -> retry, then dead-letter

    logging.info("message %s: %s", msg.get("messageId"), result)
    return "", 204  # 2xx = ack
