{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "4157a1e5-b5cc-4de1-89ef-28ae59af104e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from mlflow.deployments import get_deploy_client\n",
    "client = get_deploy_client(\"databricks\")\n",
    "\n",
    "client.create_endpoint(\n",
    "    name=\"fda-rag-endpoint\",\n",
    "    config={\n",
    "        \"served_entities\": [{\n",
    "            \"name\": \"fda-assistant-v1\",\n",
    "            \"entity_name\": \"fda_rag.gold.fda_assistant\",\n",
    "            \"entity_version\": \"1\",\n",
    "            \"workload_size\": \"Small\",\n",
    "            \"scale_to_zero_enabled\": True\n",
    "        }],\n",
    "        \"auto_capture_config\": {\n",
    "            \"catalog_name\": \"fda_rag\",\n",
    "            \"schema_name\": \"gold\",\n",
    "            \"table_name_prefix\": \"fda_rag_inference\"\n",
    "        }\n",
    "    })\n",
    "\n",
    "print(\"Endpoint creation submitted. ~10 min to ready.\")"
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
   "notebookName": "06_deploy_serving.py",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
