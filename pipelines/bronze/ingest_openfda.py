"""
Bronze pipeline — openFDA raw ingestion.

Replaces notebook 01_ingest_bronze. Append-only, immutable system-of-record.
Output: fda_rag.bronze.openfda_raw
"""
import dlt
import requests, json, time
from pyspark.sql import functions as F

DRUGS = ["metformin", "atorvastatin", "lisinopril", "warfarin",
         "ibuprofen", "amlodipine", "omeprazole", "sertraline"]


def fetch(url, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 404:
                return []
            r.raise_for_status()
            return r.json().get("results", [])
        except requests.RequestException:
            if i == retries - 1:
                return []
            time.sleep(2 ** i)


def fetch_labels(drug, limit=100):
    return fetch(f"https://api.fda.gov/drug/label.json?"
                 f"search=openfda.generic_name:{drug}&limit={limit}")


def fetch_events(drug, limit=100):
    return fetch(f"https://api.fda.gov/drug/event.json?"
                 f"search=patient.drug.medicinalproduct:{drug}&limit={limit}")


@dlt.table(
    name="openfda_raw",
    comment="Raw openFDA JSON — append-only, immutable system-of-record.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect("payload_non_empty", "length(payload) > 0")
@dlt.expect("source_valid", "source IN ('label', 'event')")
def openfda_raw():
    records = []
    for d in DRUGS:
        for label in fetch_labels(d):
            records.append({"drug": d, "source": "label", "payload": json.dumps(label)})
        time.sleep(1)
        for ev in fetch_events(d):
            records.append({"drug": d, "source": "event", "payload": json.dumps(ev)})
        time.sleep(1)

    return (spark.createDataFrame(records)
            .withColumn("ingested_at", F.current_timestamp()))
