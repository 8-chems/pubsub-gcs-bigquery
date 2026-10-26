"""Manually publish a fake GCS event, useful for testing without uploading a file.

The object must already exist in the bucket:
  python publisher.py --bucket my-project-landing --object incoming/orders.csv
In normal operation you never run this: the bucket notification publishes for you.
"""

import argparse
import json
import os

from google.cloud import pubsub_v1

project = os.environ["GCP_PROJECT"]
parser = argparse.ArgumentParser()
parser.add_argument("--topic", default="file-events")
parser.add_argument("--bucket", required=True)
parser.add_argument("--object", required=True)
parser.add_argument(
    "--generation",
    default="1",
    help="real value: gcloud storage objects describe ... --format='value(generation)'",
)
args = parser.parse_args()

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project, args.topic)

payload = {"bucket": args.bucket, "name": args.object}
future = publisher.publish(
    topic_path,
    data=json.dumps(payload).encode("utf-8"),  # body must be bytes
    # Attributes must be strings. These mirror what GCS sets.
    eventType="OBJECT_FINALIZE",
    bucketId=args.bucket,
    objectId=args.object,
    objectGeneration=args.generation,
)
print("Published message id:", future.result())  # blocks until the server confirms
