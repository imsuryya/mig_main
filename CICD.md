# CI/CD — Migration Platform (Phase 4)

CI/CD for this repo lives in a **separate repo**: `migration-platform-cicd`
(scaffolded at `C:\Git\mig-cicd`). This repo only fires a notification on
push to `main` — no deploy credentials, environment gates, or Databricks
config live here.

```
migration-platform (this repo)  --push to main-->  repository_dispatch
                                                          |
                                                          v
                                        migration-platform-cicd
                                        runs `databricks bundle deploy`
```

See [../mig-cicd/README.md](../mig-cicd/README.md) for the full setup
checklist (secrets, GitHub Environments, service principals). What's
relevant on **this** repo's side:

## This repo's part

1. Get this repo on GitHub:
   ```bash
   cd C:\Git\mig
   git init
   git add .
   git commit -m "Initial migration platform scaffold"
   gh repo create <your-org>/migration-platform --private --source=. --push
   ```
2. Add these to **this repo's** Settings → Secrets and variables → Actions:
   | Type | Name | Value |
   |---|---|---|
   | Secret | `CICD_REPO_PAT` | GitHub PAT with `repo` write access to `migration-platform-cicd` |
   | Variable | `CICD_REPO` | `<your-org>/migration-platform-cicd` |
3. Fill in the placeholders in [databricks.yml](databricks.yml): the
   `<your-workspace>` host (yours: `dbc-11cf3253-4503.cloud.databricks.com`)
   and the `<staging-sp-application-id>` / `<prod-sp-application-id>`
   service principal IDs — the CI/CD repo's workflow checks out this repo
   and runs `databricks bundle deploy` against these targets.
4. [.github/workflows/notify-deploy.yml](.github/workflows/notify-deploy.yml)
   is already in place — nothing else to add here.

Once both repos are set up, push to `main` and check the **Actions** tab on
`migration-platform-cicd` for the deploy run.

## What's not automated yet

- `src/schema.sql` (the six UC tables) stays a manual, one-time step per
  environment — see [DEPLOYMENT.md](DEPLOYMENT.md).
- No automated tests before deploy beyond the `dev` smoke-test job.
- No rollback step — redeploy a previous ref manually via the CI/CD repo's
  `workflow_dispatch` if a bad deploy reaches prod.
