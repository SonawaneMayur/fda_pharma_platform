# Declarative Pipelines (Lakeflow / DLT)

Spark Declarative Pipeline equivalent of notebooks 01–02c. One pipeline per medallion layer — each layer is independently deployable, schedulable, and ownable, which is the industry-standard pattern for production medallion DLT.

## Layout

```
pipelines/
├── bronze/
│   ├── ingest_openfda.py        # replaces notebook 01
│   └── pipeline_settings.json
├── silver/
│   ├── conform_entities.py      # replaces notebook 02a
│   └── pipeline_settings.json
├── gold_rag/
│   ├── build_chunks.py          # replaces notebook 02b
│   └── pipeline_settings.json
└── gold_analytics/
    ├── build_star_schema.py     # replaces notebook 02c
    └── pipeline_settings.json
```

## Outputs (identical to the imperative notebooks)

| Pipeline | Catalog.Schema | Tables |
|---|---|---|
| `fda_bronze_ingest`      | `fda_rag.bronze` | `openfda_raw` |
| `fda_silver_conform`     | `fda_rag.silver` | `drug_labels`, `adverse_events` |
| `fda_gold_rag_chunks`    | `fda_rag.gold`   | `fda_chunks` (CDF enabled) |
| `fda_gold_analytics_star`| `fda_rag.gold`   | `dim_drug`, `dim_patient`, `dim_reaction`, `dim_date`, `fact_adverse_event` |

Cross-pipeline reads use `spark.read.table("fda_rag.<schema>.<table>")` because `dlt.read()` only sees tables in the current pipeline.

## What DLT adds over the imperative notebooks

| Capability | Mechanism in this repo |
|---|---|
| Data quality enforcement | `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail` decorators |
| Auto schema evolution | Default DLT behavior |
| Lineage in Unity Catalog | Auto-generated from `dlt.read()` graph |
| Incremental processing | Materialized views auto-detect what changed |
| Backfills & full refresh | UI button per pipeline |
| DQ metrics dashboard | Built-in per-expectation pass/fail counts |

## Data quality expectations applied

**Bronze** — lenient (raw must accept what arrives):
- `payload_non_empty`: warn if payload is empty
- `source_valid`: warn if source is not `label` or `event`

**Silver** — drop bad rows:
- `drug_generic_present`: drop labels with no generic name
- `safetyreportid_present`: drop events with no report ID
- `reactions_non_empty`: drop events with no reactions

**Gold** — fail pipeline if violated (keys must be valid):
- `drug_key_unique`, `patient_key_present`, `reaction_key_present`, `date_key_present`
- Fact: `drug_key_resolved`, `reaction_key_resolved` (drop unresolved joins)

## Deployment

### Option 1 — UI (recommended for first deploy)

For each of the 4 pipelines:
1. Upload the `.py` file to your workspace under `/Workspace/Users/<you>/fda_pharma_platform/pipelines/<layer>/`
2. **Workflows → Delta Live Tables → Create pipeline**
3. Use the values from the matching `pipeline_settings.json`:
   - Name (`fda_bronze_ingest`, etc.)
   - Catalog: `fda_rag`
   - Target schema: `bronze` / `silver` / `gold`
   - Source code: path to the `.py` file
   - Channel: Current, Edition: Advanced, Photon: enabled
4. Click **Start**

### Option 2 — Databricks CLI

After editing each `pipeline_settings.json` to set the correct workspace path for your user:

```bash
# Update the workspace path placeholder
sed -i '' "s|<your-user>@<your-domain>|$(whoami)@your-domain.com|g" \
  pipelines/*/pipeline_settings.json

# Create each pipeline
for layer in bronze silver gold_rag gold_analytics; do
  databricks pipelines create --json @pipelines/$layer/pipeline_settings.json
done
```

### Option 3 — Asset Bundles (recommended for production)

Wrap all 4 pipelines plus the imperative ML notebooks (03-07) in a single `databricks.yml` bundle for promotion across dev → staging → prod.

## Execution order

```
fda_bronze_ingest          → fda_rag.bronze.openfda_raw
        ↓
fda_silver_conform         → fda_rag.silver.{drug_labels, adverse_events}
        ↓                 ↓
fda_gold_rag_chunks    fda_gold_analytics_star
```

Bronze and Silver are sequential. Both Gold pipelines can run in parallel — they only depend on Silver.

To orchestrate the chain, wrap all 4 in a single **Databricks Workflow** with these task dependencies:

```
bronze → silver → [gold_rag, gold_analytics]
```

## What stays imperative (not DLT)

Notebooks 00, 03, 04, 05, 06, 07 are **not** converted because they aren't ETL:

| Notebook | Why it stays imperative |
|---|---|
| 00_setup            | One-time catalog/schema/volume creation |
| 03_vector_index     | Uses `VectorSearchClient` SDK, not a table transform |
| 04_rag_chain        | LangChain + MLflow model registration |
| 05_evaluate         | `mlflow.evaluate` against a live endpoint |
| 06_deploy_serving   | Creates a Model Serving endpoint |
| 07_monitoring       | Creates a view; could be moved to DLT later if desired |

## What I'd change for prod scale

- **Bronze**: replace direct HTTP fetch with Auto Loader on a landing volume that a separate ingestion job populates. DLT excels at file-source ingestion, not API calls.
- **Silver**: replace `udf(parse_label)` with native `from_json(schema)` — UDFs are slow.
- **Gold analytics**: add SCD2 on `dim_drug` using `apply_changes` (`SCD TYPE 2`) to track manufacturer changes over time.
- **CDC for Silver from Bronze**: use streaming tables (`@dlt.table` over `spark.readStream`) so Silver is incremental, not full-refresh.
