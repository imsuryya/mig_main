# Databricks notebook source
# Phase 1, ingest job — task 3: fold the analyzer's complexity scores back
# into source_objects. The row itself was already created at upload time
# (see app/app.py) — this task only fills in complexity_score, matched by
# the file's Volume path.

dbutils.widgets.text("catalog", "migration_platform")
dbutils.widgets.text("schema", "core")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table = f"{catalog}.{schema}.source_objects"

report = dbutils.jobs.taskValues.get(taskKey="run_analyzer", key="analyzer_report", default=[])

if not report:
    print("No analyzer report rows found — nothing to update.")
else:
    updates = spark.createDataFrame(
        [{"raw_path": r["file"], "complexity_score": r.get("complexity_score")} for r in report]
    )
    updates.createOrReplaceTempView("analyzer_updates")

    spark.sql(f"""
        MERGE INTO {table} AS t
        USING analyzer_updates AS u
        ON t.raw_path = u.raw_path
        WHEN MATCHED THEN UPDATE SET t.complexity_score = u.complexity_score
    """)
    print(f"Updated complexity_score for {len(report)} object(s) in {table}")
