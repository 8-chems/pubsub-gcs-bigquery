"""Streaming-pull subscriber. Runs anywhere with credentials: laptop, VM, GKE, Cloud Run."""

import logging
import os

from common import PROJECT_ID, handle_event
from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO)
SUBSCRIPTION = os.environ.get("SUBSCRIPTION", "file-events-sub")


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        result = handle_event(dict(message.attributes))
        logging.info("message %s: %s", message.message_id, result)
        message.ack()  # only after the work succeeded
    except Exception:
        logging.exception("message %s failed, will be redelivered", message.message_id)
        message.nack()  # redeliver now (subject to retry backoff); DLQ after max attempts


def main() -> None:
    subscriber = pubsub_v1.SubscriberClient()
    path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION)

    # Backpressure: never hold more than 10 unacked messages in memory.
    flow_control = pubsub_v1.types.FlowControl(max_messages=10)
    future = subscriber.subscribe(path, callback=callback, flow_control=flow_control)
    logging.info("Listening on %s", path)

    with subscriber:
        try:
            future.result()
        except KeyboardInterrupt:
            future.cancel()
            future.result()


if __name__ == "__main__":
    main()
