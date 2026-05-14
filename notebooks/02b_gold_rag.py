{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "29227e45-0baa-4542-aee6-ac963fad41b9",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "Gold — RAG projection"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "f61bd4be-6399-42ab-9998-1af264a25e57",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql import functions as F\n",
    "\n",
    "LABEL_SECTIONS = [\"warnings\", \"adverse_reactions\", \"contraindications\",\n",
    "                  \"drug_interactions\", \"dosage_and_administration\",\n",
    "                  \"warnings_and_cautions\", \"boxed_warning\"]\n",
    "\n",
    "labels = spark.table(\"fda_rag.silver.drug_labels\")\n",
    "events = spark.table(\"fda_rag.silver.adverse_events\")\n",
    "\n",
    "label_chunks_dfs = []\n",
    "for section in LABEL_SECTIONS:\n",
    "    df = (labels\n",
    "        .filter(F.col(section).isNotNull() & (F.length(section) > 50))\n",
    "        .select(\n",
    "            F.col(\"drug_brand\"),\n",
    "            F.col(\"drug_generic\"),\n",
    "            F.lit(section).alias(\"section\"),\n",
    "            F.lit(\"label\").alias(\"source_type\"),\n",
    "            F.substring(F.col(section), 1, 4000).alias(\"text\")\n",
    "        ))\n",
    "    label_chunks_dfs.append(df)\n",
    "\n",
    "label_chunks = label_chunks_dfs[0]\n",
    "for d in label_chunks_dfs[1:]:\n",
    "    label_chunks = label_chunks.unionByName(d)\n",
    "\n",
    "event_chunks = (events\n",
    "    .withColumn(\"text\", F.concat(\n",
    "        F.lit(\"Patient age \"),\n",
    "        F.coalesce(F.col(\"patient_age\").cast(\"string\"), F.lit(\"unknown\")),\n",
    "        F.lit(\", sex \"), F.col(\"patient_sex\"),\n",
    "        F.lit(\". Serious: \"), F.coalesce(F.col(\"serious\"), F.lit(\"unknown\")),\n",
    "        F.lit(\". Reported reactions: \"),\n",
    "        F.array_join(F.col(\"reactions\"), \", \")\n",
    "    ))\n",
    "    .select(\n",
    "        F.col(\"drug_name\").alias(\"drug_brand\"),\n",
    "        F.col(\"drug_name\").alias(\"drug_generic\"),\n",
    "        F.lit(\"adverse_event_report\").alias(\"section\"),\n",
    "        F.lit(\"event\").alias(\"source_type\"),\n",
    "        F.col(\"text\")\n",
    "    )\n",
    "    .filter(F.length(\"text\") > 50))\n",
    "\n",
    "chunks = (label_chunks.unionByName(event_chunks)\n",
    "    .withColumn(\"chunk_id\",\n",
    "        F.sha2(F.concat_ws(\"|\", \"drug_generic\", \"section\", \"text\"), 256))\n",
    "    .dropDuplicates([\"chunk_id\"]))\n",
    "\n",
    "(chunks.write.mode(\"overwrite\")\n",
    "    .option(\"delta.enableChangeDataFeed\", \"true\")\n",
    "    .option(\"overwriteSchema\", \"true\")\n",
    "    .saveAsTable(\"fda_rag.gold.fda_chunks\"))\n",
    "\n",
    "print(f\"Gold chunks: {spark.table('fda_rag.gold.fda_chunks').count()}\")"
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
   "notebookName": "02b_gold_rag.py",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
