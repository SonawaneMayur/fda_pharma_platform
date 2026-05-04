{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b5669293-6330-4d27-9778-cf9faa2d0809",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Enable inference logging — done already via auto_capture_config in Step 6\n",
    "# Inference table: fda_rag.gold.fda_rag_inference_payload\n",
    "\n",
    "# Build operational metrics view\n",
    "spark.sql(\"\"\"\n",
    "CREATE OR REPLACE VIEW fda_rag.gold.v_inference_metrics AS\n",
    "SELECT\n",
    "  date_trunc('hour', from_unixtime(timestamp_ms/1000)) AS hour,\n",
    "  COUNT(*) AS request_count,\n",
    "  AVG(execution_time_ms) AS avg_latency_ms,\n",
    "  PERCENTILE(execution_time_ms, 0.50) AS p50_latency_ms,\n",
    "  PERCENTILE(execution_time_ms, 0.95) AS p95_latency_ms,\n",
    "  SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS errors,\n",
    "  SUM(CASE WHEN response LIKE '%don''t have enough%' THEN 1 ELSE 0 END) AS refusals\n",
    "FROM fda_rag.gold.fda_rag_inference_payload\n",
    "GROUP BY 1\n",
    "\"\"\")\n",
    "\n",
    "display(spark.sql(\"SELECT * FROM fda_rag.gold.v_inference_metrics ORDER BY hour DESC LIMIT 24\"))"
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
   "notebookName": "07_monitoring.py",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
