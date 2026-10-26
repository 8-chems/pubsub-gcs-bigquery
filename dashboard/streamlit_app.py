"""Streamlit dashboard for the GCS -> Pub/Sub -> BigQuery pipeline.

Run from the repository root:
    streamlit run dashboard/streamlit_app.py

It uses YOUR Application Default Credentials (gcloud auth application-default login).
It is a local learning tool: do not expose it on a public URL, it acts with your permissions.
"""

import io
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
from helpers import describe_status, load_job_id, validate_csv

SAMPLE_DIR = Path(__file__).resolve().parent.parent / "sample-data"
SAFE_NAME = re.compile(r"^[A-Za-z0-9_\-]+$")

st.set_page_config(page_title="Pub/Sub ingestion", page_icon="📬", layout="wide")
st.title("📬 Pub/Sub ingestion pipeline")
st.caption("Cloud Storage → Pub/Sub → subscriber → BigQuery, with a dead-letter topic.")

# ---------------------------------------------------------------- sidebar
with st.sidebar:
    st.header("Configuration")
    project = st.text_input("Project ID", os.environ.get("GCP_PROJECT", ""))
    bucket_name = st.text_input(
        "Landing bucket",
        os.environ.get("BUCKET") or (f"{project}-landing" if project else ""),
    )
    location = st.text_input("BigQuery location", os.environ.get("BQ_LOCATION", "EU"))
    dataset = st.text_input("Dataset", "raw")
    table_name = st.text_input("Table", "orders")
    st.subheader("Subscriptions to monitor")
    dlq_sub = st.text_input("Dead-letter subscription", "file-events-dlq-sub")
    st.caption("Metrics are read for `file-events-sub` and `file-events-push-sub`.")

if not project:
    st.info("Enter your project ID in the sidebar (or set `GCP_PROJECT` before starting).")
    st.stop()

if not all(SAFE_NAME.match(x) for x in (project, dataset, table_name)):
    st.error("Project, dataset and table may only contain letters, digits, '-' and '_'.")
    st.stop()


@st.cache_resource
def get_clients(project_id: str, bq_location: str) -> dict:
    from google.cloud import bigquery, monitoring_v3, pubsub_v1, storage

    return {
        "storage": storage.Client(project=project_id),
        "bq": bigquery.Client(project=project_id, location=bq_location),
        "subscriber": pubsub_v1.SubscriberClient(),
        "monitoring": monitoring_v3.MetricServiceClient(),
    }


try:
    gcp = get_clients(project, location)
except Exception as exc:  # noqa: BLE001 - show any credential problem to the user
    st.error(
        f"Could not create Google Cloud clients: {exc}\n\n"
        "Run `gcloud auth application-default login`, then restart Streamlit."
    )
    st.stop()

FULL_TABLE = f"`{project}.{dataset}.{table_name}`"
tab_upload, tab_health, tab_data, tab_dlq = st.tabs(
    ["⬆️ Upload", "🩺 Pipeline health", "📊 Data", "☠️ Dead letters"]
)


# ---------------------------------------------------------------- upload tab
@st.fragment(run_every=5)
def upload_tracker() -> None:
    """Re-runs every 5 s on its own: follows each upload through the pipeline."""
    from google.api_core.exceptions import NotFound

    tracked = st.session_state.get("tracked", [])
    if not tracked:
        st.info("Files you send appear here, with live status.")
        return
    for item in tracked[:6]:
        age = int((datetime.now(timezone.utc) - item["sent_at"]).total_seconds())
        try:
            job = gcp["bq"].get_job(item["job_id"])
            message = (job.error_result or {}).get("message")
            level, text = describe_status(True, job.state, message, job.output_rows)
        except NotFound:
            level, text = describe_status(False)
        except Exception as exc:  # noqa: BLE001
            level, text = "error", f"Could not read the load job: {exc}"
        line = f"**{item['uri']}** ({age}s ago)\n\n{text}"
        {"ok": st.success, "error": st.error}.get(level, st.info)(line)
        st.caption(f"BigQuery job: `{item['job_id']}`")


with tab_upload:
    st.subheader("Send a CSV into the pipeline")
    st.caption(
        f"The file is written to `gs://{bucket_name}/incoming/`. Cloud Storage then publishes "
        "an event to Pub/Sub, and the subscriber loads the file into BigQuery."
    )
    uploaded = st.file_uploader("CSV file", type="csv")
    sample = st.selectbox(
        "...or use a sample",
        ["(none)", "orders.csv (valid)", "bad_orders.csv (will fail: tests the dead-letter path)"],
    )

    name, data = None, None
    if uploaded is not None:
        name, data = uploaded.name, uploaded.getvalue()
    elif sample != "(none)":
        name = sample.split(" ")[0]
        data = (SAMPLE_DIR / name).read_bytes()

    if data is not None:
        text = data.decode("utf-8-sig", errors="replace")
        try:
            st.dataframe(pd.read_csv(io.StringIO(text), dtype=str).head(20))
        except Exception:  # noqa: BLE001 - a malformed file is exactly what we want to demo
            st.code(text[:1000])
        problems = validate_csv(text)
        force = False
        if problems:
            st.warning("This file will probably fail in BigQuery:\n\n- " + "\n- ".join(problems))
            force = st.checkbox("Upload it anyway (to watch retries and the dead-letter topic)")
        else:
            st.success("The file matches the table schema.")
        unique = st.checkbox(
            "Add a timestamp to the file name",
            value=True,
            help="Re-uploading the same name is a new object generation: rows are loaded again.",
        )
        if st.button("Send to pipeline", type="primary", disabled=bool(problems) and not force):
            stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            stem = Path(name).stem
            object_name = f"incoming/{stem}-{stamp}.csv" if unique else f"incoming/{stem}.csv"
            try:
                blob = gcp["storage"].bucket(bucket_name).blob(object_name)
                blob.upload_from_string(data, content_type="text/csv")
            except Exception as exc:  # noqa: BLE001
                st.error(f"Upload failed: {exc}")
            else:
                uri = f"gs://{bucket_name}/{object_name}"
                st.session_state.setdefault("tracked", []).insert(
                    0,
                    {
                        "uri": uri,
                        "job_id": load_job_id(uri, str(blob.generation)),
                        "sent_at": datetime.now(timezone.utc),
                    },
                )
                st.toast(f"Uploaded {object_name}")

    st.divider()
    st.subheader("Live status")
    upload_tracker()

    with st.expander("Latest files in incoming/"):
        try:
            blobs = list(
                gcp["storage"].list_blobs(bucket_name, prefix="incoming/", max_results=200)
            )
            latest = sorted(blobs, key=lambda b: b.updated, reverse=True)[:15]
            st.dataframe(
                pd.DataFrame(
                    [{"object": b.name, "size_bytes": b.size, "updated": b.updated} for b in latest]
                )
            )
        except Exception as exc:  # noqa: BLE001
            st.warning(f"Could not list the bucket: {exc}")


# ---------------------------------------------------------------- health tab
@st.cache_data(ttl=30, show_spinner=False)
def read_metric(_client, project_id: str, metric_type: str, minutes: int, how: str) -> dict:
    """Read one Cloud Monitoring metric, one number per subscription."""
    from google.cloud import monitoring_v3

    end = int(time.time())
    interval = monitoring_v3.TimeInterval(
        {"end_time": {"seconds": end}, "start_time": {"seconds": end - minutes * 60}}
    )
    series = _client.list_time_series(
        request={
            "name": f"projects/{project_id}",
            "filter": f'metric.type = "{metric_type}"',
            "interval": interval,
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }
    )
    out = {}
    for ts in series:
        values = [p.value.int64_value for p in ts.points]
        if values:  # newest point first
            out[ts.resource.labels.get("subscription_id", "?")] = (
                values[0] if how == "latest" else sum(values)
            )
    return out


with tab_health:
    st.subheader("Is the pipeline healthy?")
    st.caption(
        "Healthy: backlog near 0, oldest unacked age small, no dead letters. "
        "Metrics arrive with a delay of a minute or two."
    )
    if st.button("Refresh metrics"):
        st.cache_data.clear()
    try:
        prefix = "pubsub.googleapis.com/subscription/"
        monitoring = gcp["monitoring"]
        backlog = read_metric(
            monitoring, project, prefix + "num_undelivered_messages", 15, "latest"
        )
        oldest = read_metric(
            monitoring, project, prefix + "oldest_unacked_message_age", 15, "latest"
        )
        dead = read_metric(monitoring, project, prefix + "dead_letter_message_count", 60, "sum")
        names = sorted(set(backlog) | set(oldest) | set(dead))
        if not names:
            st.info("No metrics yet. Upload a file and check again in a minute or two.")
        else:
            st.dataframe(
                pd.DataFrame(
                    {
                        "subscription": names,
                        "backlog (messages)": [backlog.get(n, 0) for n in names],
                        "oldest unacked (s)": [oldest.get(n, 0) for n in names],
                        "dead-lettered (last hour)": [dead.get(n, 0) for n in names],
                    }
                ),
                hide_index=True,
            )
            if any(v > 300 for v in oldest.values()):
                st.warning("A message has waited over 5 minutes: is a subscriber running?")
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Could not read Cloud Monitoring (needs roles/monitoring.viewer): {exc}")
    st.caption("Full time series: Cloud Monitoring → Dashboards → Pub/Sub ingestion pipeline.")


# ---------------------------------------------------------------- data tab
@st.cache_data(ttl=20, show_spinner=False)
def run_query(_bq, sql: str) -> list[dict]:
    return [dict(row.items()) for row in _bq.query(sql).result()]


with tab_data:
    st.subheader(f"What is in {dataset}.{table_name}?")
    if st.button("Refresh data"):
        st.cache_data.clear()
    try:
        summary = run_query(
            gcp["bq"],
            f"SELECT COUNT(*) AS rows_loaded, ROUND(SUM(amount), 2) AS revenue, "
            f"COUNT(DISTINCT customer_id) AS customers FROM {FULL_TABLE}",
        )[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("Rows loaded", summary["rows_loaded"])
        c2.metric("Revenue", summary["revenue"] or 0)
        c3.metric("Customers", summary["customers"])

        daily = pd.DataFrame(
            run_query(
                gcp["bq"],
                f"SELECT DATE(created_at) AS day, COUNT(*) AS orders FROM {FULL_TABLE} "
                "GROUP BY day ORDER BY day",
            )
        )
        if not daily.empty:
            st.bar_chart(daily.set_index("day")["orders"])
        st.dataframe(
            pd.DataFrame(
                run_query(
                    gcp["bq"], f"SELECT * FROM {FULL_TABLE} ORDER BY created_at DESC LIMIT 50"
                )
            )
        )
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Could not query BigQuery: {exc}")

    with st.expander("Recent load jobs created by the subscriber"):
        try:
            jobs = run_query(
                gcp["bq"],
                f"SELECT creation_time, job_id, state, error_result.message AS error "
                f"FROM `region-{location.lower()}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT "
                "WHERE job_type = 'LOAD' AND job_id LIKE 'load_%' "
                "ORDER BY creation_time DESC LIMIT 25",
            )
            st.dataframe(pd.DataFrame(jobs))
        except Exception as exc:  # noqa: BLE001
            st.warning(f"Could not read job history: {exc}")


# ---------------------------------------------------------------- dead-letter tab
with tab_dlq:
    st.subheader("Messages that failed too many times")
    st.caption(
        "Peeking reads up to 10 messages and immediately releases them, so nothing is removed."
    )
    if st.button("Peek at the dead-letter subscription"):
        from google.api_core.exceptions import DeadlineExceeded

        path = f"projects/{project}/subscriptions/{dlq_sub}"
        try:
            response = gcp["subscriber"].pull(
                request={"subscription": path, "max_messages": 10}, timeout=20
            )
        except DeadlineExceeded:
            response = None
        except Exception as exc:  # noqa: BLE001
            st.error(f"Could not pull from {dlq_sub}: {exc}")
            response = None
        messages = list(response.received_messages) if response else []
        if not messages:
            st.success("No dead-lettered messages right now.")
        else:
            gcp["subscriber"].modify_ack_deadline(
                request={
                    "subscription": path,
                    "ack_ids": [m.ack_id for m in messages],
                    "ack_deadline_seconds": 0,
                }
            )
            st.dataframe(
                pd.DataFrame(
                    [
                        {"message_id": m.message.message_id, **dict(m.message.attributes)}
                        for m in messages
                    ]
                )
            )
            st.caption(
                "`CloudPubSubDeadLetterSourceDeliveryCount` is how many times Pub/Sub tried. "
                "To reprocess a file, re-upload the corrected version."
            )
