# Databricks notebook source
# Phase 1/2 — transpile job: run Lakebridge transpile per pending object,
# write parsed_ir + generated_artifacts rows.

import subprocess
import uuid
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "migration_platform")
dbutils.widgets.text("schema", "core")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

pending = spark.sql(f"""
    SELECT object_id, source_system, object_type, raw_path
    FROM {catalog}.{schema}.source_objects so
    WHERE NOT EXISTS (
        SELECT 1 FROM {catalog}.{schema}.generated_artifacts ga
        WHERE ga.object_id = so.object_id
    )
""").collect()

# COMMAND ----------
IR_VERSION = "v1"
now = lambda: datetime.now(timezone.utc)

for obj in pending:
    output_dir = f"/Volumes/{catalog}/{schema}/generated/{obj.object_id}"
    dbutils.fs.mkdirs(output_dir)

    result = subprocess.run(
        [
            "databricks", "labs", "lakebridge", "transpile",
            "--source-dialect", obj.source_system,
            "--input-source", f"/dbfs{obj.raw_path}",
            "--output-folder", f"/dbfs{output_dir}",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"[{obj.object_id}] transpile failed: {result.stderr}")
        continue

    # Lakebridge writes one output file per input; take the first match.
    output_files = dbutils.fs.ls(output_dir)
    if not output_files:
        print(f"[{obj.object_id}] transpile produced no output — skipping")
        continue

    with open(f"/dbfs{output_files[0].path}") as f:
        generated_code = f.read()

    is_incomplete = "INCOMPLETE" in generated_code or "TODO" in generated_code
    target_shape = "sql" if obj.object_type == "stored_procedure" else "pyspark"

    spark.sql(f"""
        INSERT INTO {catalog}.{schema}.parsed_ir (object_id, ir_version, ir_payload, parsed_at)
        VALUES ('{obj.object_id}', '{IR_VERSION}', '', current_timestamp())
    """)
    spark.createDataFrame([{
        "artifact_id": str(uuid.uuid4()),
        "object_id": obj.object_id,
        "target_shape": target_shape,
        "code": generated_code,
        "generator": "lakebridge",
        "confidence_note": "flagged incomplete by transpiler" if is_incomplete else None,
        "generated_at": now(),
    }]).write.mode("append").saveAsTable(f"{catalog}.{schema}.generated_artifacts")

    print(f"[{obj.object_id}] transpiled -> {target_shape}" + (" (incomplete, needs AI-assist)" if is_incomplete else ""))

print(f"Processed {len(pending)} object(s).")
