# Deployment Map

Where every file in this repo ends up in Databricks, and how it gets there.

## One-time manual step (before any bundle deploy)

| File | Where to paste / run it | How |
|---|---|---|
| `src/schema.sql` | A Databricks SQL warehouse editor (or a notebook cell) | Copy-paste and run once. Creates the `migration_platform.core` catalog/schema and the six tables. Not part of the bundle — DDL runs once, ahead of everything else (Phase 0 step 1). |

## Everything else — deployed automatically via the bundle

Run from the repo root:

```
databricks configure                        # Phase 0 step 2, one time
databricks bundle validate --target dev
databricks bundle deploy --target dev        # or --target staging / prod
```

No manual copy-pasting needed past this point — `bundle deploy` reads `databricks.yml` + `resources/*.yml` and pushes each piece to the right place.

| Local file(s) | Deployed to | Databricks object |
|---|---|---|
| `databricks.yml` | n/a (bundle root config, stays local) | defines `root_path`: `/Workspace/Users/<you>/.bundle/migration_platform/<target>` |
| `resources/jobs.yml` | Workflows → Jobs | 3 Lakeflow Jobs: `migration_platform_ingest_and_profile`, `migration_platform_transpile`, `migration_platform_reconcile` |
| `src/jobs/stage_to_volume.py` | `<root_path>/files/src/jobs/` as a notebook | Task 1 of `ingest_and_profile` |
| `src/jobs/run_lakebridge_analyzer.py` | same | Task 2 of `ingest_and_profile` |
| `src/jobs/write_source_objects.py` | same | Task 3 of `ingest_and_profile` |
| `src/jobs/run_lakebridge_transpile.py` | same | Task 1 of `transpile` |
| `src/jobs/ai_assist.py` | same | Task 2 of `transpile` |
| `src/jobs/ai_code_review.py` | same | Task 3 of `transpile` |
| `src/jobs/run_lakebridge_reconcile.py` | same | Task 1 of `reconcile` |
| `src/skills/*.md` | `<root_path>/files/src/skills/` | Read at runtime by `ai_assist.py` / `ai_code_review.py` — not a Databricks object itself, just synced files |
| `resources/apps.yml` | Compute → Apps | Defines the two apps below |
| `app/` (app.py, app.yaml, requirements.txt) | Deployed as the **migration-platform** Databricks App | Review UI (Phase 1). Needs a SQL warehouse attached as a resource in the app config in the workspace UI after first deploy |
| `mcp_server/` (server.py, requirements.txt) | Deployed as the **migration-platform-mcp** Databricks App | MCP endpoint (Phase 3) — point Claude Code / Claude Desktop's MCP config at this app's URL |

## Secrets / endpoints — set up manually in the workspace, not in the bundle

| What | Where | Phase |
|---|---|---|
| Gemini API key | `databricks secrets create-scope migration-platform` then `databricks secrets put-secret migration-platform gemini-api-key` — **never paste the raw key in chat, a doc, or a commit; only into this command or the UI's secret field** | Phase 0 step 5 |
| Gemini model-serving endpoint | Serving → Create serving endpoint → **Custom Provider** (OpenAI-compatible), base URL `https://generativelanguage.googleapis.com/v1beta/openai/`, Bearer token = `{{secrets/migration-platform/gemini-api-key}}`, model e.g. `gemini-2.5-flash`. Name it to match `ai_endpoint_name` in `databricks.yml` (`gemini-migration-assist`). A Google AI Studio key uses this Custom Provider path, not the Vertex AI provider (that one needs a GCP service-account key instead) — [Databricks docs](https://docs.databricks.com/aws/en/machine-learning/model-serving/query-gemini-api) | Phase 0 step 5 |
| SQL warehouse for the app | App's Settings → Resources tab (after first `apps deploy`), bind a warehouse so `DATABRICKS_HTTP_PATH` in `app.yaml` resolves | Phase 1 step 5 |

## CI/CD — split across two repos, see CICD.md

This repo (`mig_main`) only fires a `repository_dispatch` on push to `main`
via `.github/workflows/notify-deploy.yml`. The actual `databricks bundle
deploy` runs in the separate `migcicd` repo. See [CICD.md](CICD.md) for the
full setup.
