# FDA Pharmacovigilance Platform on Databricks

A production-grade reference for two consumers (GenAI/LLM and Analytics) of FDA adverse event data, served from a single medallion lakehouse — with two implementation paths (imperative notebooks + Lakeflow Declarative Pipelines), event-driven orchestration, multi-tenant row-level security, and Asset Bundle deployment.

## Business problem

Pharmacovigilance teams spend hours per safety inquiry searching FAERS and drug labels. Epidemiologists need slice-and-dice analytics over the same data. Both consumers historically built parallel, divergent pipelines off the same source.

## Approach

- **Bronze**: Immutable openFDA JSON, append-only with ingestion timestamp.
- **Silver**: Conformed entities — `drug_labels`, `adverse_events`. Source-agnostic, business-agnostic, deduplicated.
- **Gold (RAG)**: Section-aware chunks → Databricks Vector Search → grounded RAG chain.
- **Gold (Analytics)**: Star schema (4 dims, 1 fact) → Databricks SQL dashboard.
- Both Gold layers derive from the **same Silver** — business logic defined exactly once.

See the full solution write-up — problem statement, architecture, decisions, deployment lifecycle, and step-by-step implementation playbook:
- [Solution Design (PDF)](FDA%20Pharmacovigilance%20Platform%20%E2%80%94%20Solution%20Design.pdf) — print-friendly, shareable

## Repository structure

```
fda_pharma_platform/
├── notebooks/             Imperative reference implementation (00–08)
├── pipelines/             Lakeflow Declarative Pipelines (DLT) — one per medallion layer
│   ├── bronze/            ingest_openfda.py + smart polling check
│   ├── silver/            conform_entities.py
│   ├── gold_rag/          build_chunks.py
│   └── gold_analytics/    build_star_schema.py
├── workflows/             Event-driven orchestration JSON (cron + table_update triggers)
├── resources/             Asset Bundle resource definitions (pipelines / workflows / serving)
├── databricks.yml         Asset Bundle root manifest (dev / staging / prod targets)
├── api/                   FastAPI gateway (API-key auth, request IDs, structured logs)
├── eval/                  Eval set with ground_truth for mlflow.evaluate
├── sql/                   Star-schema analytics queries
└── FDA Pharmacovigilance Platform — Solution Design.pdf   ↑ same content, PDF export
```

## Two implementation paths

The medallion layers are implemented twice — pick the path that fits your context:

| Path | Files | When to use |
|---|---|---|
| **Imperative** | `notebooks/01–02c` | Prototyping, demos, ad-hoc exploration |
| **Declarative (DLT)** | `pipelines/` + `workflows/` | Production ETL with DQ expectations, lineage, incremental processing |

Both produce identical Unity Catalog tables. Downstream consumers (Vector Search index, RAG chain, FastAPI gateway, SQL dashboard) work unchanged regardless of which path you deploy.

ML lifecycle (notebooks 03–07) stays imperative — model registration, evaluation, and serving aren't ETL.

## Event-driven orchestration

Cron is used **only** at Bronze, gated by a smart polling task that checks `meta.results.total` on openFDA against a state table. Silver and Gold pipelines fire automatically via Unity Catalog `table_update` triggers when their upstream tables commit.

- Bronze runs only when openFDA actually changes → ~50–60% compute savings vs unconditional daily runs
- Silver fires within ~1–2 min of Bronze commit
- Both Gold workflows fire in parallel when Silver commits

See [workflows/README.md](workflows/README.md) for the full pattern.

## Multi-tenant governance

Row-level security via UC row filter + column mask:

- **Tenant axis**: drug therapy area (cardio / metabolic / psych / OTC)
- **Row filter**: `tenant_drug_filter()` applied to Silver and Gold tables that have `drug_generic`
- **PII mask**: `age_pii_mask()` on `silver.adverse_events.patient_age` — raw age for clinical group, decade buckets for everyone else
- **RAG-specific**: Vector indexes don't inherit row filters — apply tenant filter at retrieval time via VS metadata filter (`filter="drug_generic IN (...)"`)

One-time setup: [notebooks/08_setup_rls.py.ipynb](notebooks/08_setup_rls.py.ipynb).

## Deployment

Asset Bundles for repeatable promotion across environments:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run fda_workflow_event_driven -t dev
databricks bundle deploy -t staging
databricks bundle deploy -t prod
```

Three targets defined in [databricks.yml](databricks.yml):
- `dev` — paused schedules, dev catalog (`fda_rag_dev`), per-user resource prefixes
- `staging` — service principal, staging catalog
- `prod` — service principal, prod catalog (`fda_rag`), `Medium` workload size

See [resources/README.md](resources/README.md) for full deployment walkthrough and CI/CD wiring.

## Stack

- **Storage / governance**: Unity Catalog, Delta Lake (Change Data Feed enabled where needed)
- **ETL**: Lakeflow Declarative Pipelines (DLT) with `@dlt.expect_or_drop` / `@dlt.expect_or_fail` quality gates
- **Orchestration**: Databricks Workflows — cron + smart polling + `table_update` triggers
- **Vector**: Databricks Vector Search with Delta Sync index (TRIGGERED)
- **Foundation models**: `databricks-gpt-5-5-pro` (LLM) + `databricks-gte-large-en` (embeddings)
- **ML lifecycle**: MLflow Experiments + Model Registry in Unity Catalog
- **Serving**: Databricks Model Serving with scale-to-zero + auto-capture inference table
- **API**: FastAPI gateway
- **Deployment**: Databricks Asset Bundles (dev / staging / prod targets)
- **Compute**: Photon-enabled Serverless clusters

## Results

- RAG faithfulness: 0.87, relevance: 0.91, p95 latency: 2.4s
- Refusal rate on out-of-corpus questions: 94%
- Analytics dashboard: 4 widgets, sub-second response on parameterized filter
- Single Silver layer feeds both consumers — zero duplicated business logic
- Compute savings from event-driven orchestration: ~50–60% vs unconditional daily runs

## Tradeoffs I made

- **Section-aware chunking** vs naive token splits: +18% faithfulness; custom logic per source type.
- **`databricks-gpt-5-5-pro`** vs alternatives: strong instruction-following and citation format; tradeoff documented for Llama 3.1 70B / Mixtral.
- **Triggered VS sync** vs continuous: cost-controlled; FDA updates daily.
- **SHA-256 surrogate keys** vs identity columns: deterministic and idempotent for portfolio purposes; production would use SCD2 with identity keys.
- **Age banding in `dim_patient`** + **column mask on `silver.adverse_events.patient_age`**: privacy-by-design at the model layer rather than at the query layer.
- **Drug-therapy-area** as the tenant axis (vs manufacturer): cleaner mapping to real pharma team structure and works with the existing data attributes.
- **Query-time VS filter** vs per-tenant indexes: 1× index cost + filter overhead is cheaper than N indexes for typical tenancy size.
- **Cron only at Bronze**, table-update triggers downstream: each layer decoupled from upstream timing.
- **Four separate DLT pipelines** vs one mega-pipeline: each layer independently deployable, ownable, and scalable.

## What I'd do next

- Hybrid retrieval (BM25 + vector) for rare-drug recall
- LLM-as-judge eval at scale with human spot-checks
- SCD2 on `dim_drug` to track manufacturer changes over time
- Per-tenant vector indexes for regulated-isolation use cases
- Native streaming Silver from Bronze CDF (currently full-refresh materialized views)
- Vector Search endpoint + index managed via Terraform / Pulumi (not natively in Asset Bundles yet)
- CI/CD pipeline for `databricks bundle deploy` on PR merge
- Lakehouse Monitoring on the inference table for automated drift / refusal-rate alerts
