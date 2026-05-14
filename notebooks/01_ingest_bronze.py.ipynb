{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "5e8730c6-ee87-4ce3-a9ec-096af8ebd25c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import requests, json, time\n",
    "from pyspark.sql import functions as F\n",
    "\n",
    "DRUGS = [\"metformin\", \"atorvastatin\", \"lisinopril\", \"warfarin\",\n",
    "         \"ibuprofen\", \"amlodipine\", \"omeprazole\", \"sertraline\"]\n",
    "\n",
    "def fetch(url, retries=3):\n",
    "    for i in range(retries):\n",
    "        try:\n",
    "            r = requests.get(url, timeout=30)\n",
    "            if r.status_code == 404:\n",
    "                return []\n",
    "            r.raise_for_status()\n",
    "            return r.json().get(\"results\", [])\n",
    "        except requests.RequestException as e:\n",
    "            if i == retries - 1:\n",
    "                print(f\"FAILED: {url} \u2014 {e}\")\n",
    "                return []\n",
    "            time.sleep(2 ** i)\n",
    "\n",
    "def fetch_labels(drug, limit=100):\n",
    "    return fetch(f\"https://api.fda.gov/drug/label.json?\"\n",
    "                 f\"search=openfda.generic_name:{drug}&limit={limit}\")\n",
    "\n",
    "def fetch_events(drug, limit=100):\n",
    "    return fetch(f\"https://api.fda.gov/drug/event.json?\"\n",
    "                 f\"search=patient.drug.medicinalproduct:{drug}&limit={limit}\")\n",
    "\n",
    "records = []\n",
    "for d in DRUGS:\n",
    "    print(f\"Fetching {d}...\")\n",
    "    for label in fetch_labels(d):\n",
    "        records.append({\"drug\": d, \"source\": \"label\", \"payload\": json.dumps(label)})\n",
    "    time.sleep(1)\n",
    "    for ev in fetch_events(d):\n",
    "        records.append({\"drug\": d, \"source\": \"event\", \"payload\": json.dumps(ev)})\n",
    "    time.sleep(1)\n",
    "\n",
    "print(f\"Total records: {len(records)}\")\n",
    "\n",
    "df = (spark.createDataFrame(records)\n",
    "        .withColumn(\"ingested_at\", F.current_timestamp()))\n",
    "\n",
    "# Bronze is append-only; Silver deduplicates by safetyreportid / drug_generic+manufacturer\n",
    "(df.write.mode(\"append\")\n",
    "   .saveAsTable(\"fda_rag.bronze.openfda_raw\"))\n",
    "\n",
    "print(\"Bronze counts by source:\")\n",
    "spark.sql(\"SELECT source, COUNT(*) FROM fda_rag.bronze.openfda_raw GROUP BY source\").show()"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "5"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "01_ingest_bronze.py",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}