# Databricks notebook source
# Phase 2 — AI code-review task: independent Claude call comparing source
# logic to generated code, producing the confidence_note that drives the
# "yellow flag" in the review app.

dbutils.widgets.text("catalog", "migration_platform")
dbutils.widgets.text("schema", "core")
dbutils.widgets.text("claude_endpoint", "claude-migration-assist")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
endpoint = dbutils.widgets.get("claude_endpoint")

from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")

REVIEW_PROMPT = """You are reviewing a migrated code artifact for logical
equivalence with its source. Compare the two and respond with exactly two
lines:
CONFIDENCE: high | medium | low
NOTE: <one sentence on any discrepancy or missing logic, or "looks equivalent">
Do not rewrite the code."""


def review(source_code: str, generated_code: str) -> str:
    response = client.predict(
        endpoint=endpoint,
        inputs={
            "messages": [
                {"role": "system", "content": REVIEW_PROMPT},
                {"role": "user", "content": f"SOURCE:\n{source_code}\n\nGENERATED:\n{generated_code}"},
            ],
            "max_tokens": 512,
        },
    )
    return response["choices"][0]["message"]["content"].strip()


# COMMAND ----------
needs_review = spark.sql(f"""
    SELECT ga.artifact_id, ga.code AS generated_code, so.raw_path
    FROM {catalog}.{schema}.generated_artifacts ga
    JOIN {catalog}.{schema}.source_objects so ON so.object_id = ga.object_id
    WHERE ga.confidence_note IS NULL
""").collect()

for row in needs_review:
    with open(f"/dbfs{row.raw_path}") as f:
        source_code = f.read()

    note = review(source_code, row.generated_code)
    spark.sql(f"""
        UPDATE {catalog}.{schema}.generated_artifacts
        SET confidence_note = '{note.replace("'", "''")}'
        WHERE artifact_id = '{row.artifact_id}'
    """)
    print(f"[{row.artifact_id}] {note.splitlines()[0] if note else ''}")

print(f"Reviewed {len(needs_review)} artifact(s).")
