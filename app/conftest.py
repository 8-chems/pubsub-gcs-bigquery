import os

# common.py reads this at import time; tests never talk to GCP.
os.environ.setdefault("GCP_PROJECT", "test-project")
