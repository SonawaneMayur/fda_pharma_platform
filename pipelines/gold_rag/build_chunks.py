"""
Gold RAG pipeline — section-aware chunks for vector indexing.

Replaces notebook 02b_gold_rag. Reads cross-pipeline from Silver.
Output: fda_rag.gold.fda_chunks  (Change Data Feed enabled for Vector Search sync)
"""
import dlt
from pyspark.sql import functions as F


LABEL_SECTIONS = ["warnings", "adverse_reactions", "contraindications",
                  "drug_interactions", "dosage_and_administration",
                  "warnings_and_cautions", "boxed_warning"]


@dlt.view
def _silver_labels():
    return spark.read.table("fda_rag.silver.drug_labels")


@dlt.view
def _silver_events():
    return spark.read.table("fda_rag.silver.adverse_events")


@dlt.table(
    name="fda_chunks",
    comment="Section-aware chunks for vector search retrieval.",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_or_drop("text_non_trivial", "length(text) > 50")
@dlt.expect_or_drop("chunk_id_present", "chunk_id IS NOT NULL")
@dlt.expect("source_type_valid", "source_type IN ('label', 'event')")
def fda_chunks():
    labels = dlt.read("_silver_labels")
    events = dlt.read("_silver_events")

    label_chunks_dfs = []
    for section in LABEL_SECTIONS:
        df = (labels
              .filter(F.col(section).isNotNull() & (F.length(section) > 50))
              .select(
                  F.col("drug_brand"),
                  F.col("drug_generic"),
                  F.lit(section).alias("section"),
                  F.lit("label").alias("source_type"),
                  F.substring(F.col(section), 1, 4000).alias("text"),
              ))
        label_chunks_dfs.append(df)

    label_chunks = label_chunks_dfs[0]
    for d in label_chunks_dfs[1:]:
        label_chunks = label_chunks.unionByName(d)

    event_chunks = (events
        .withColumn("text", F.concat(
            F.lit("Patient age "),
            F.coalesce(F.col("patient_age").cast("string"), F.lit("unknown")),
            F.lit(", sex "), F.col("patient_sex"),
            F.lit(". Serious: "), F.coalesce(F.col("serious"), F.lit("unknown")),
            F.lit(". Reported reactions: "),
            F.array_join(F.col("reactions"), ", "),
        ))
        .select(
            F.col("drug_name").alias("drug_brand"),
            F.col("drug_name").alias("drug_generic"),
            F.lit("adverse_event_report").alias("section"),
            F.lit("event").alias("source_type"),
            F.col("text"),
        )
        .filter(F.length("text") > 50))

    return (label_chunks.unionByName(event_chunks)
            .withColumn("chunk_id",
                F.sha2(F.concat_ws("|", "drug_generic", "section", "text"), 256))
            .dropDuplicates(["chunk_id"]))
