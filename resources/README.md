# Asset Bundle Resources

Resource definitions split by concern. The root [`databricks.yml`](../databricks.yml) includes everything in this folder via `include: resources/*.yml`.

```
resources/
├── pipelines.yml      4 DLT pipelines (bronze / silver / gold_rag / gold_analytics)
├── workflows.yml      Scheduled bronze + 3 table-triggered + 1 ML lifecycle
├── serving.yml        Registered model + Model Serving endpoint with auto-capture
└── README.md          this file
```

## Deploy walkthrough

### 1. Install CLI and authenticate

```bash
brew install databricks
databricks configure --token
```

### 2. Edit `databricks.yml`
Replace `<your-dev-workspace>`, `<your-staging-workspace>`, `<your-prod-workspace>` with real workspace URLs. Set `notifications_email`.

### 3. Validate
```bash
databricks bundle validate -t dev
```
The CLI parses every YAML file, resolves variable substitution, and reports schema errors before any API calls. **Always run this before deploy.**

### 4. Deploy to dev
```bash
databricks bundle deploy -t dev
```

This creates (in dev mode):
- All 4 DLT pipelines (prefixed with your email: `[your@email.com] [dev] fda_bronze_ingest`)
- All 4 workflows (paused so they don't auto-run in dev)
- Registered model + serving endpoint (named `fda-rag-endpoint-dev`)
- Workspace artifacts uploaded to `/Workspace/Users/<you>/.bundle/fda_pharma_platform/dev/`

### 5. Run one workflow on demand
```bash
databricks bundle run fda_workflow_event_driven -t dev
```

### 6. Promote to staging then prod
After eval gates pass:
```bash
databricks bundle deploy -t staging
databricks bundle deploy -t prod
```
Prod uses a service principal (`fda-pharma-prod-sp`) so deployment is auditable and not tied to a personal user.

### 7. Tear down (when done with dev)
```bash
databricks bundle destroy -t dev
```

## Variable substitution

Variables are defined in `databricks.yml` under `variables:` and overridden per target. They're referenced as `${var.name}` throughout the YAML — e.g., `${var.catalog}.bronze.openfda_raw`.

| Variable | dev value | prod value |
|---|---|---|
| `catalog` | `fda_rag_dev` | `fda_rag` |
| `workload_size` | `Small` | `Medium` |
| `llm_endpoint` | `databricks-gpt-5-5-pro` | `databricks-gpt-5-5-pro` |

This means **the same YAML deploys to different catalogs in different environments**. No code changes, no copy-paste.

## What's NOT in the bundle (and why)

- **Vector Search endpoint** — Asset Bundles don't natively support `vector_search_endpoints` resource yet. Create it once via UI or REST API; bundle assumes it exists at `${var.vector_search_endpoint}`.
- **Vector Search index** — same. Create via `notebook 03_vector_index` and treat as a one-time setup.
- **UC catalogs / schemas** — bundles deploy *into* a catalog; they don't create the catalog itself. Run `00_setup` once.
- **RLS row filters / column masks** — bundles can create UC functions, but the `ALTER TABLE … SET ROW FILTER` is an imperative DDL that bundles don't track natively. Run `08_setup_rls` post-deploy.

The pattern: **bundles for repeatable resources; one-time setup notebooks for foundational state.**

## CI/CD wiring (sketch)

```yaml
# .github/workflows/deploy.yml
name: Deploy FDA Bundle
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - name: Validate
        run: databricks bundle validate -t prod
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_PROD_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_PROD_TOKEN }}
      - name: Deploy
        run: databricks bundle deploy -t prod --auto-approve
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_PROD_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_PROD_TOKEN }}
```
