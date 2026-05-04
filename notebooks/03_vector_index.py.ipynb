{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "dbc85825-6776-4cca-8e0d-b2e85a34fab5",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from databricks.vector_search.client import VectorSearchClient\n",
    "\n",
    "vsc = VectorSearchClient()\n",
    "\n",
    "vsc.create_delta_sync_index(\n",
    "    endpoint_name=\"fda-vs-endpoint\",\n",
    "    index_name=\"fda_rag.gold.fda_chunks_index\",\n",
    "    source_table_name=\"fda_rag.gold.fda_chunks\",\n",
    "    pipeline_type=\"TRIGGERED\",\n",
    "    primary_key=\"chunk_id\",\n",
    "    embedding_source_column=\"text\",\n",
    "    embedding_model_endpoint_name=\"databricks-gte-large-en\"\n",
    ")\n",
    "\n",
    "print(\"Index creation submitted. Wait 5-10 min for initial sync.\")\n",
    "print(\"Check status: Compute → Vector Search → fda-vs-endpoint\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "567a3f25-240b-4adb-928c-18132560441a",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "After it syncs, sanity check:"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b4c33cb6-4488-4596-9d06-d502d903afeb",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "results = vsc.get_index(\"fda-vs-endpoint\", \"fda_rag.gold.fda_chunks_index\").similarity_search(\n",
    "    query_text=\"What are cardiac side effects of statins?\",\n",
    "    columns=[\"drug_generic\", \"section\", \"text\"],\n",
    "    num_results=3)\n",
    "for r in results['result']['data_array']:\n",
    "    print(r[:2], r[2][:200], \"\\n\")"
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
   "notebookName": "03_vector_index.py",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
