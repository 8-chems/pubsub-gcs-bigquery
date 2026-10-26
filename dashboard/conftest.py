import os
import sys
from pathlib import Path

# helpers.py lives next to this file; common.py lives in ../app (parity test only).
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "app"))
os.environ.setdefault("GCP_PROJECT", "test-project")
