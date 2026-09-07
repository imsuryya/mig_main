# Databricks notebook source
# Phase 1, ingest job — task 2: run Lakebridge's analyzer over newly staged
# files and pass the parsed report forward to write_source_objects.py.
#
# Requires the Lakebridge CLI on the cluster's PATH. If you smoke-tested it
# locally (Phase 0 step 4) but not on a job cluster yet, install it via a
# cluster-scoped init script that runs the same `databricks labs install
# lakebridge` command.

import json
import subprocess

dbutils.widgets.text("catalog", "migration_platform")
dbutils.widgets.text("schema", "core")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_path = f"/Volumes/{catalog}/{schema}/staged_sources"
report_path = f"{volume_path}/analyzer_report.json"

# COMMAND ----------
result = subprocess.run(
    [
        "databricks", "labs", "lakebridge", "analyze",
        "--source-directory", f"/dbfs{volume_path}",
        "--report-file", f"/dbfs{report_path}",
    ],
    capture_output=True,
    text=True,
)
if result.returncode != 0:
    raise RuntimeError(f"lakebridge analyze failed:\n{result.stderr}")

# COMMAND ----------
with open(f"/dbfs{report_path}") as f:
    report = json.load(f)

# Expected shape (per Lakebridge docs — adjust if your installed version differs):
# [{"file": "...", "object_name": "...", "complexity_score": 0.0}, ...]
dbutils.jobs.taskValues.set(key="analyzer_report", value=report)
print(f"Analyzed {len(report)} object(s); passed to write_source_objects via task value.")
