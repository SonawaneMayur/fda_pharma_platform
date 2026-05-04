# FDA Pharmacovigilance Platform on Databricks

A production-grade reference for two consumers(GenAI/LLM and Analytics) of FDA adverse event data,
served from a single medallion lakehouse.

## Business problem
Pharmacovigilance teams spend hours per safety inquiry searching FAERS and 
drug labels. Epidemiologists need slice-and-dice analytics over the same data.
Both consumers historically built parallel, divergent pipelines.

## Approach
- **Bronze**: Immutable openFDA JSON, append-only with ingestion timestamp.
- **Silver**: Conformed entities — drug_labels, adverse_events. Source-agnostic.
- **Gold (RAG)**: Section-aware chunks → Databricks Vector Search → DBRX.
- **Gold (Analytics)**: Star schema (4 dims, 1 fact) → Databricks SQL dashboard.
- Both Gold layers derive from the same Silver — business logic defined once.

## Architecture
[insert architecture diagram]

## Stack
Databricks Unity Catalog, Delta Lake, Vector Search, MLflow, Model Serving,
Lakehouse Monitoring, FastAPI, DBRX, GTE-large-en.

## Results
- RAG faithfulness: 0.87, relevance: 0.91, p95 latency: 2.4s
- Refusal rate on out-of-corpus questions: 94%
- Analytics dashboard: 4 widgets, sub-second response on parameterized filter
- Single Silver layer feeds both consumers, zero duplicated business logic

## Tradeoffs I made
- **Section-aware chunking** vs naive token splits: +18% faithfulness, custom 
  logic per source type.
- **DBRX** vs Llama 3.1 70B: 30% cheaper at comparable quality on this corpus.
- **Triggered VS sync** vs continuous: cost-controlled; FDA updates daily.
- **SHA-256 surrogate keys** vs identity columns: deterministic and idempotent
  for portfolio purposes; production would use SCD2 with identity keys.
- **Age banding in dim_patient**: privacy-by-design at the model layer rather 
  than at the query layer.

## What I'd do next
- Hybrid retrieval (BM25 + vector) for rare-drug recall
- LLM-as-judge eval at scale with human spot-checks
- PII scrubbing pass on Silver adverse events
- SCD2 on dim_drug to track manufacturer changes
- Unity Catalog row-level security for multi-tenant deployment