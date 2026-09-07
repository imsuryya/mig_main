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
    # Two conventions coexist under src/skills/:
    #   flat single-file  — e.g. oracle-procedural-pyspark.md — written as a
    #     terse, code-only completion skill; fits this job's one-shot use.
    #   migration/skills/<source>-to-sdp/SKILL.md — the richer *-to-sdp
    #     family (shared Databricks/SDP target docs live once in
    #     migration/references/, reused by every sibling skill). NOTE: these
    #     are planning skills (their deliverable is a written migration plan,
    #     code only as a follow-up) — built for interactive/agentic use, not
    #     really a one-shot fill-in-the-blank call like this job makes. It's
    #     wired in below for completeness; expect it to behave more like a
    #     detailed design brief than a drop-in code generator here.
    candidates = [
        f"{source_system}-{object_type}-{target_shape}",
        f"{source_system}-{target_shape}",
        f"{source_system}-procedural-{target_shape}" if object_type == "stored_procedure" else None,
    ]
    for name in filter(None, candidates):
        path = f"../skills/{name}.md"
        try:
            with open(path) as f:
                skill = f.read()
        except FileNotFoundError:
            continue
        return skill + "\n\n" + load_referenced_docs(path)

    sdp_path = f"../skills/migration/skills/{source_system}-to-sdp/SKILL.md"
    try:
        with open(sdp_path) as f:
            skill = f.read()
        return skill + "\n\n" + load_referenced_docs(sdp_path)
    except FileNotFoundError:
        pass

    return "Convert the source object into the target shape as faithfully as possible."


def load_referenced_docs(skill_path: str) -> str:
    """Skill files point at reference docs by relative path — either
    skill-local (`references/<dir>/<file>.md`) or, for the *-to-sdp family,
    shared docs one level up (`../../references/<file>.md`). Pull each one's
    full text in so the model actually has that material, not just a
    pointer to it."""
    import os as _os
    import re

    with open(skill_path) as f:
        text = f.read()

    skill_dir = _os.path.dirname(skill_path)
    doc_paths = sorted(set(re.findall(r"`((?:\.\./)*references/[\w./-]+\.md)`", text)))

    sections = []
    for rel_path in doc_paths:
        full_path = _os.path.normpath(_os.path.join(skill_dir, rel_path))
        try:
            with open(full_path) as f:
                sections.append(f"## Reference: {rel_path}\n\n{f.read()}")
        except FileNotFoundError:
            print(f"Warning: {skill_path} references missing doc {rel_path}")
    return "\n\n".join(sections)


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
