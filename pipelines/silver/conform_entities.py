"""
Silver pipeline — conformed entities from Bronze JSON.

Replaces notebook 02a_silver. Reads cross-pipeline from fda_rag.bronze.openfda_raw.
Outputs: fda_rag.silver.drug_labels, fda_rag.silver.adverse_events
"""
import dlt
import json
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                                IntegerType, ArrayType)


label_schema = StructType([
    StructField("drug_brand", StringType()),
    StructField("drug_generic", StringType()),
    StructField("manufacturer", StringType()),
    StructField("warnings", StringType()),
    StructField("adverse_reactions", StringType()),
    StructField("contraindications", StringType()),
    StructField("drug_interactions", StringType()),
    StructField("dosage_and_administration", StringType()),
    StructField("warnings_and_cautions", StringType()),
    StructField("boxed_warning", StringType()),
])


def parse_label(payload_json):
    p = json.loads(payload_json)
    openfda = p.get("openfda", {}) or {}

    def first(d, k):
        v = d.get(k)
        return v[0] if isinstance(v, list) and v else None

    def join_field(k):
        v = p.get(k)
        if isinstance(v, list):
            return " ".join(str(x) for x in v).strip() or None
        return v

    return {
        "drug_brand": first(openfda, "brand_name"),
        "drug_generic": first(openfda, "generic_name"),
        "manufacturer": first(openfda, "manufacturer_name"),
        "warnings": join_field("warnings"),
        "adverse_reactions": join_field("adverse_reactions"),
        "contraindications": join_field("contraindications"),
        "drug_interactions": join_field("drug_interactions"),
        "dosage_and_administration": join_field("dosage_and_administration"),
        "warnings_and_cautions": join_field("warnings_and_cautions"),
        "boxed_warning": join_field("boxed_warning"),
    }


event_schema = StructType([
    StructField("safetyreportid", StringType()),
    StructField("drug_name", StringType()),
    StructField("patient_age", IntegerType()),
    StructField("patient_sex", StringType()),
    StructField("serious", StringType()),
    StructField("reactions", ArrayType(StringType())),
    StructField("receive_date", StringType()),
])


SEX_MAP = {"1": "male", "2": "female", "0": "unknown"}


def parse_event(payload_json):
    p = json.loads(payload_json)
    patient = p.get("patient", {}) or {}
    drugs = patient.get("drug", []) or []
    drug_name = drugs[0].get("medicinalproduct") if drugs else None
    reactions = [r.get("reactionmeddrapt") for r in patient.get("reaction", [])
                 if r.get("reactionmeddrapt")]
    age_raw = patient.get("patientonsetage")
    try:
        age = int(age_raw) if age_raw is not None else None
    except (ValueError, TypeError):
        age = None
    return {
        "safetyreportid": p.get("safetyreportid"),
        "drug_name": drug_name,
        "patient_age": age,
        "patient_sex": SEX_MAP.get(str(patient.get("patientsex", "")), "unknown"),
        "serious": p.get("serious"),
        "reactions": reactions,
        "receive_date": p.get("receivedate"),
    }


parse_label_udf = F.udf(parse_label, label_schema)
parse_event_udf = F.udf(parse_event, event_schema)


@dlt.view
def _bronze_source():
    """Cross-pipeline read from the Bronze pipeline's output table."""
    return spark.read.table("fda_rag.bronze.openfda_raw")


@dlt.table(
    name="drug_labels",
    comment="Parsed, deduplicated drug labels — single source of truth for label data.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("drug_generic_present", "drug_generic IS NOT NULL")
@dlt.expect("has_any_clinical_text",
            "warnings IS NOT NULL OR adverse_reactions IS NOT NULL OR contraindications IS NOT NULL")
def drug_labels():
    return (dlt.read("_bronze_source")
            .filter(F.col("source") == "label")
            .withColumn("parsed", parse_label_udf("payload"))
            .select("drug", "parsed.*", F.current_timestamp().alias("ingested_at"))
            .dropDuplicates(["drug_generic", "manufacturer"]))


@dlt.table(
    name="adverse_events",
    comment="Parsed, deduplicated adverse-event safety reports.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("safetyreportid_present", "safetyreportid IS NOT NULL")
@dlt.expect_or_drop("reactions_non_empty", "size(reactions) > 0")
@dlt.expect("patient_sex_known", "patient_sex IN ('male', 'female', 'unknown')")
def adverse_events():
    return (dlt.read("_bronze_source")
            .filter(F.col("source") == "event")
            .withColumn("parsed", parse_event_udf("payload"))
            .select("drug", "parsed.*", F.current_timestamp().alias("ingested_at"))
            .dropDuplicates(["safetyreportid"]))
