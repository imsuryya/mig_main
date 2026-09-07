# Databricks notebook source
# Phase 2 — AI-assist task: for objects Lakebridge flagged incomplete, call
# the governed Claude model-serving endpoint with the matching migration
# skill (versioned files under src/skills/).

import uuid
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "migration_platform")
dbutils.widgets.text("schema", "core")
dbutils.widgets.text("claude_endpoint", "claude-migration-assist")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
endpoint = dbutils.widgets.get("claude_endpoint")

from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")


def load_skill(source_system: str, object_type: str, target_shape: str) -> str:
    # Matches file naming in src/skills/, e.g. oracle-procedural-pyspark.md
    candidates = [
        f"{source_system}-{object_type}-{target_shape}",
        f"{source_system}-procedural-{target_shape}" if object_type == "stored_procedure" else None,
    ]
    for name in filter(None, candidates):
        try:
            with open(f"../skills/{name}.md") as f:
                return f.read()
        except FileNotFoundError:
            continue
    return "Convert the source object into the target shape as faithfully as possible."


def ai_complete(source_system: str, object_type: str, target_shape: str, ir_payload: str, generated_code: str) -> str:
    system_prompt = load_skill(source_system, object_type, target_shape)
    response = client.predict(
        endpoint=endpoint,
        inputs={
            "messages": [
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": (
                        f"The transpiler produced this incomplete {target_shape} code. "
                        f"Fill in the incomplete parts only, keep everything else unchanged.\n\n"
                        f"IR:\n{ir_payload}\n\nGenerated code:\n{generated_code}"
                    ),
                },
            ],
            "max_tokens": 4096,
        },
    )
    return response["choices"][0]["message"]["content"]


# COMMAND ----------
flagged = spark.sql(f"""
    SELECT ga.artifact_id, ga.object_id, ga.target_shape, ga.code,
           so.source_system, so.object_type,
           COALESCE(pi.ir_payload, '') AS ir_payload
    FROM {catalog}.{schema}.generated_artifacts ga
    JOIN {catalog}.{schema}.source_objects so ON so.object_id = ga.object_id
    LEFT JOIN {catalog}.{schema}.parsed_ir pi ON pi.object_id = ga.object_id
    WHERE ga.generator = 'lakebridge' AND ga.confidence_note LIKE '%incomplete%'
""").collect()

now = lambda: datetime.now(timezone.utc)

for row in flagged:
    improved_code = ai_complete(
        row.source_system, row.object_type, row.target_shape, row.ir_payload, row.code
    )
    spark.createDataFrame([{
        "artifact_id": str(uuid.uuid4()),
        "object_id": row.object_id,
        "target_shape": row.target_shape,
        "code": improved_code,
        "generator": "ai_assist",
        "confidence_note": None,  # filled in by ai_code_review.py next
        "generated_at": now(),
    }]).write.mode("append").saveAsTable(f"{catalog}.{schema}.generated_artifacts")
    print(f"[{row.object_id}] AI-assist produced a revised artifact")

print(f"Processed {len(flagged)} flagged artifact(s).")
