"""
Gold Analytics pipeline — star schema (4 dims + 1 fact) for dashboards.

Replaces notebook 02c_gold_analytics. Reads cross-pipeline from Silver.
Outputs: fda_rag.gold.dim_drug, dim_patient, dim_reaction, dim_date, fact_adverse_event
"""
import dlt
from pyspark.sql import functions as F


BODY_SYSTEMS = {
    "cardiac": ["cardiac", "heart", "myocardial", "arrhythmia",
                "tachycardia", "bradycardia", "hypertension", "hypotension"],
    "gastrointestinal": ["nausea", "vomiting", "diarrhoea", "diarrhea",
                          "abdominal", "gastric", "constipation"],
    "neurological": ["headache", "dizziness", "seizure", "tremor",
                      "neuropathy", "paraesthesia"],
    "hepatic": ["hepatic", "liver", "jaundice", "transaminase"],
    "renal": ["renal", "kidney", "creatinine"],
    "dermatological": ["rash", "pruritus", "urticaria", "dermatitis"],
    "respiratory": ["dyspnoea", "dyspnea", "cough", "asthma", "pulmonary"],
    "haematological": ["anaemia", "anemia", "thrombocytopenia",
                        "leukopenia", "bleeding", "haemorrhage"],
}


def age_band(c):
    return (F.when(c.isNull(), "unknown")
             .when(c < 18, "0-17").when(c < 45, "18-44")
             .when(c < 65, "45-64").when(c < 75, "65-74")
             .otherwise("75+"))


def classify_body_system(col):
    expr = F.lit("other")
    for system, kws in BODY_SYSTEMS.items():
        cond = F.lit(False)
        for kw in kws:
            cond = cond | F.lower(col).contains(kw)
        expr = F.when(cond, system).otherwise(expr)
    return expr


@dlt.view
def _silver_labels():
    return spark.read.table("fda_rag.silver.drug_labels")


@dlt.view
def _silver_events():
    return spark.read.table("fda_rag.silver.adverse_events")


# -------- dim_drug --------
@dlt.table(
    name="dim_drug",
    comment="Drug dimension — conformed across label and event sources.",
    table_properties={"quality": "gold"},
)
@dlt.expect_or_fail("drug_key_unique", "drug_key IS NOT NULL")
def dim_drug():
    labels = dlt.read("_silver_labels")
    events = dlt.read("_silver_events")

    drugs_from_labels = (labels.select(
            F.lower(F.col("drug_generic")).alias("drug_generic"),
            F.col("drug_brand"), F.col("manufacturer"))
        .filter(F.col("drug_generic").isNotNull()))

    drugs_from_events = (events.select(
            F.lower(F.col("drug_name")).alias("drug_generic"),
            F.lit(None).cast("string").alias("drug_brand"),
            F.lit(None).cast("string").alias("manufacturer"))
        .filter(F.col("drug_generic").isNotNull()))

    return (drugs_from_labels.unionByName(drugs_from_events)
            .groupBy("drug_generic")
            .agg(F.first("drug_brand", ignorenulls=True).alias("drug_brand"),
                 F.first("manufacturer", ignorenulls=True).alias("manufacturer"))
            .withColumn("drug_key", F.sha2(F.col("drug_generic"), 256))
            .select("drug_key", "drug_generic", "drug_brand", "manufacturer")
            .withColumn("loaded_at", F.current_timestamp()))


# -------- dim_patient --------
@dlt.table(
    name="dim_patient",
    comment="Patient dimension — age-banded for privacy-by-design.",
    table_properties={"quality": "gold"},
)
@dlt.expect_or_fail("patient_key_present", "patient_key IS NOT NULL")
@dlt.expect("age_band_known",
            "age_band IN ('0-17','18-44','45-64','65-74','75+','unknown')")
def dim_patient():
    return (dlt.read("_silver_events")
            .select(age_band(F.col("patient_age")).alias("age_band"),
                    F.coalesce(F.col("patient_sex"), F.lit("unknown")).alias("sex"))
            .distinct()
            .withColumn("patient_key",
                F.sha2(F.concat_ws("|", "age_band", "sex"), 256))
            .select("patient_key", "age_band", "sex")
            .withColumn("loaded_at", F.current_timestamp()))


# -------- dim_reaction --------
@dlt.table(
    name="dim_reaction",
    comment="Reaction dimension — classified into body systems.",
    table_properties={"quality": "gold"},
)
@dlt.expect_or_fail("reaction_key_present", "reaction_key IS NOT NULL")
def dim_reaction():
    return (dlt.read("_silver_events")
            .select(F.explode("reactions").alias("reaction_term"))
            .filter(F.col("reaction_term").isNotNull())
            .select(F.lower(F.col("reaction_term")).alias("reaction_term"))
            .distinct()
            .withColumn("body_system", classify_body_system(F.col("reaction_term")))
            .withColumn("reaction_key", F.sha2(F.col("reaction_term"), 256))
            .select("reaction_key", "reaction_term", "body_system")
            .withColumn("loaded_at", F.current_timestamp()))


# -------- dim_date --------
@dlt.table(
    name="dim_date",
    comment="Date dimension covering the span of adverse-event reports.",
    table_properties={"quality": "gold"},
)
@dlt.expect_or_fail("date_key_present", "date_key IS NOT NULL")
def dim_date():
    bounds = (dlt.read("_silver_events")
              .select(F.to_date("receive_date", "yyyyMMdd").alias("d"))
              .filter(F.col("d").isNotNull())
              .agg(F.min("d").alias("min_d"), F.max("d").alias("max_d"))
              .collect()[0])

    return (spark.sql(f"""
        SELECT explode(sequence(
            to_date('{bounds['min_d']}'),
            to_date('{bounds['max_d']}'),
            interval 1 day)) AS full_date
        """)
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
        .select("date_key", "full_date", "year", "quarter",
                "month", "month_name", "day_of_week"))


# -------- fact_adverse_event --------
@dlt.table(
    name="fact_adverse_event",
    comment="One row per (report, reaction) — partitioned by date_key.",
    partition_cols=["date_key"],
    table_properties={"quality": "gold"},
)
@dlt.expect_or_drop("drug_key_resolved", "drug_key IS NOT NULL")
@dlt.expect_or_drop("reaction_key_resolved", "reaction_key IS NOT NULL")
@dlt.expect("is_serious_binary", "is_serious IN (0, 1)")
def fact_adverse_event():
    events = dlt.read("_silver_events")

    exploded = (events
        .withColumn("reaction_term", F.explode("reactions"))
        .select(
            F.col("safetyreportid"),
            F.lower(F.col("drug_name")).alias("drug_generic"),
            F.lower(F.col("reaction_term")).alias("reaction_term"),
            F.col("patient_age"), F.col("patient_sex"), F.col("serious"),
            F.to_date("receive_date", "yyyyMMdd").alias("event_date"))
        .filter(F.col("reaction_term").isNotNull())
        .withColumn("age_band", age_band(F.col("patient_age")))
        .withColumn("sex", F.coalesce(F.col("patient_sex"), F.lit("unknown"))))

    dim_drug_df = dlt.read("dim_drug").select("drug_key", "drug_generic")
    dim_reaction_df = dlt.read("dim_reaction").select("reaction_key", "reaction_term")
    dim_patient_df = dlt.read("dim_patient").select("patient_key", "age_band", "sex")

    return (exploded
            .join(dim_drug_df, on="drug_generic", how="left")
            .join(dim_reaction_df, on="reaction_term", how="left")
            .join(dim_patient_df, on=["age_band", "sex"], how="left")
            .withColumn("date_key",
                F.date_format("event_date", "yyyyMMdd").cast("int"))
            .select(
                F.col("safetyreportid").alias("report_id"),
                "drug_key", "reaction_key", "patient_key", "date_key",
                F.when(F.col("serious") == "1", 1).otherwise(0).alias("is_serious"),
                F.lit(1).alias("reaction_count")))
