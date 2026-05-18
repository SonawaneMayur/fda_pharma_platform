# Workflows — Event-Driven Orchestration

Two orchestration paths, both event-driven. They share the same shape: a smart polling gate at Bronze, then table-update triggers cascading through Silver → Gold.

```
workflows/
├── notebook_pipeline.json              ← single workflow runs all notebooks (poll → 01 → 02a → 02b/02c)
├── dlt_bronze_workflow.json            ← poll → Bronze DLT pipeline
├── dlt_silver_workflow.json            ← table-update trigger on bronze.openfda_raw
├── dlt_gold_rag_workflow.json          ← table-update trigger on silver.*
└── dlt_gold_analytics_workflow.json    ← table-update trigger on silver.*
```

## The pattern

### Bronze layer — smart polling gate
openFDA is a REST API with no webhooks. We cannot subscribe to "new data" events. Solution: every morning, a cheap task asks openFDA for `meta.results.total` per drug and compares to a state table. If unchanged, the downstream pipeline is skipped.

```
06:00 UTC  ──►  poll_openfda  ──►  gate_on_change  ──┬─► [data_changed=true]  ──►  run_bronze
                                                      └─► [data_changed=false] ──►  skip everything
```

**Cost saved**: openFDA updates ~3-4 days/week. On the other 3-4 days, the entire downstream chain stays at zero cost.

### Silver / Gold layers — table-update triggers
Once Bronze commits to `fda_rag.bronze.openfda_raw`, Databricks fires a **table_update** event. The Silver workflow listens for that event and starts automatically. When Silver commits, both Gold workflows fire in parallel.

```
bronze commit ──► silver workflow runs ──► silver commit
                                                │
                                  ┌─────────────┴─────────────┐
                                  ▼                           ▼
                       gold_rag workflow runs    gold_analytics workflow runs
                                  ▼                           ▼
                       gold_rag.fda_chunks       gold.fact_adverse_event
                                  ▼                           ▼
                       VS index auto-sync        SQL dashboard auto-refresh
```

**No tight coupling.** Each layer's workflow only knows about its upstream **table** — not about who wrote to it, when, or how often.

## Trigger types used

| Trigger | Where | Latency | Cost when idle |
|---|---|---|---|
| `schedule` (cron) | Bronze workflow | Bounded by cron | Free (no run created) |
| `condition_task` gate | Bronze workflow | ~instant inside a run | n/a |
| `table_update` | Silver, Gold × 2 | ~1-2 min after upstream commit | Free (event-driven) |

## Deployment

1. **Create the 4 DLT pipelines** first (see `pipelines/README.md`) and **record the pipeline IDs** that Databricks generates.

2. **Edit each `dlt_*_workflow.json`** — replace `<bronze-pipeline-id>`, `<silver-pipeline-id>`, etc. with the actual pipeline IDs. Replace `<your-user>@<domain>` with your workspace user path in `dlt_bronze_workflow.json` (for the polling task file).

3. **Create each workflow** via CLI or UI:

   ```bash
   # Notebook path (one workflow runs the whole thing)
   databricks jobs create --json @workflows/notebook_pipeline.json

   # DLT path (four workflows — one per layer)
   for w in dlt_bronze_workflow dlt_silver_workflow dlt_gold_rag_workflow dlt_gold_analytics_workflow; do
     databricks jobs create --json @workflows/$w.json
   done
   ```

4. **Test the cascade** — manually run the Bronze workflow once. Silver should auto-fire within 1-2 min of Bronze's commit. Then both Gold workflows fire after Silver's commit.

## Choosing notebook path vs DLT path

| Use the notebook workflow if… | Use the DLT workflows if… |
|---|---|
| You're prototyping or doing a portfolio demo | You're going to prod |
| You want a single workflow to operate | You want per-layer ownership / SLAs |
| Schema evolution is rare | Schema evolution is common (DLT handles it gracefully) |
| You don't need built-in DQ dashboard | You want @dlt.expect_* gates surfaced in UI |

Both produce identical Unity Catalog tables. You can switch between them without changing any downstream code (SQL queries, vector index, RAG chain, FastAPI gateway).

## What this design buys you

- **Cost**: pipelines only run on change days. ~50-60% compute savings vs unconditional daily runs.
- **Latency**: ~3-5 min total from Bronze trigger to Gold tables refreshed (vs unbounded with cron-only).
- **Decoupling**: each layer's workflow is independently editable, deployable, and ownable.
- **Auditability**: every trigger produces a run record. The state table `fda_rag.bronze._polling_state` plus workflow run history gives you a complete "what fired and why" timeline.

## Anti-patterns this avoids

- ❌ One mega-cron that runs everything daily unconditionally — wastes compute, duplicates Bronze rows on no-change days
- ❌ Continuous pipelines on a daily-updating source — 24/7 compute for no benefit
- ❌ Tight scheduling (e.g., Silver at 06:30, Gold at 06:45) — fragile to upstream delays
