"""
Smart polling task for the Bronze DLT workflow.

Runs BEFORE the Bronze pipeline in the same Workflow. Sets a task value
`data_changed` that a downstream condition_task gates on.

State table: fda_rag.bronze._polling_state
"""
import requests, time
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

DRUGS = ["metformin", "atorvastatin", "lisinopril", "warfarin",
         "ibuprofen", "amlodipine", "omeprazole", "sertraline"]


def total_count(source_path: str, search: str) -> int:
    url = f"https://api.fda.gov/drug/{source_path}.json?search={search}&limit=1"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 404:
            return 0
        r.raise_for_status()
        return r.json().get("meta", {}).get("results", {}).get("total", 0)
    except requests.RequestException:
        return 0


spark.sql("""
CREATE TABLE IF NOT EXISTS fda_rag.bronze._polling_state (
    source STRING,
    last_seen_count BIGINT,
    last_check_at TIMESTAMP
) USING DELTA
""")

total_labels = total_events = 0
for d in DRUGS:
    total_labels += total_count("label", f"openfda.generic_name:{d}")
    total_events += total_count("event", f"patient.drug.medicinalproduct:{d}")
    time.sleep(0.5)

prev = {r["source"]: r["last_seen_count"] for r in
        spark.table("fda_rag.bronze._polling_state").collect()}

changed = (prev.get("label_total", -1) != total_labels or
           prev.get("event_total", -1) != total_events)

now = datetime.utcnow()
new_state = spark.createDataFrame([
    ("label_total", total_labels, now),
    ("event_total", total_events, now),
], ["source", "last_seen_count", "last_check_at"])
new_state.createOrReplaceTempView("_new_state")
spark.sql("""
MERGE INTO fda_rag.bronze._polling_state t
USING _new_state s ON t.source = s.source
WHEN MATCHED THEN UPDATE SET
    last_seen_count = s.last_seen_count,
    last_check_at = s.last_check_at
WHEN NOT MATCHED THEN INSERT *
""")

print(f"Labels total: {prev.get('label_total', 'first run')} -> {total_labels}")
print(f"Events total: {prev.get('event_total', 'first run')} -> {total_events}")
print(f"data_changed = {changed}")

dbutils.jobs.taskValues.set(key="data_changed", value=str(changed).lower())
