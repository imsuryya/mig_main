-- Phase 0, step 1: Unity Catalog schema and the six platform tables.
-- Run this once via a SQL warehouse (or as a one-off job) before anything
-- else. Paste into a Databricks SQL editor / notebook cell, or run with:
--   databricks bundle run schema_setup --target dev   (if wrapped as a job)

CREATE CATALOG IF NOT EXISTS migration_platform;
CREATE SCHEMA IF NOT EXISTS migration_platform.core;

USE CATALOG migration_platform;
USE SCHEMA core;

CREATE TABLE IF NOT EXISTS source_objects (
  object_id        STRING NOT NULL,
  source_system     STRING NOT NULL,      -- e.g. oracle, alteryx, pentaho
  object_type       STRING NOT NULL,      -- stored_procedure, workflow, etc.
  object_name       STRING NOT NULL,
  raw_path          STRING NOT NULL,      -- Volume path to the staged source file
  complexity_score  DOUBLE,
  ingested_at       TIMESTAMP NOT NULL,
  CONSTRAINT source_objects_pk PRIMARY KEY (object_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS parsed_ir (
  object_id     STRING NOT NULL,
  ir_version    STRING NOT NULL,
  ir_payload    STRING NOT NULL,          -- JSON: Lakebridge/Morpheus IR
  parsed_at     TIMESTAMP NOT NULL,
  CONSTRAINT parsed_ir_pk PRIMARY KEY (object_id, ir_version)
) USING DELTA;

CREATE TABLE IF NOT EXISTS generated_artifacts (
  artifact_id      STRING NOT NULL,
  object_id        STRING NOT NULL,
  target_shape     STRING NOT NULL,       -- sql | pyspark | declarative-pipeline
  code             STRING NOT NULL,
  generator        STRING NOT NULL,       -- lakebridge | ai_assist
  confidence_note  STRING,                -- from the AI code-review skill
  generated_at     TIMESTAMP NOT NULL,
  CONSTRAINT generated_artifacts_pk PRIMARY KEY (artifact_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS validation_results (
  validation_id   STRING NOT NULL,
  artifact_id     STRING NOT NULL,
  check_type      STRING NOT NULL,        -- reconcile | unit_test | lint
  passed          BOOLEAN NOT NULL,
  details         STRING,
  validated_at    TIMESTAMP NOT NULL,
  CONSTRAINT validation_results_pk PRIMARY KEY (validation_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS review_decisions (
  decision_id   STRING NOT NULL,
  artifact_id   STRING NOT NULL,
  reviewer      STRING NOT NULL,
  decision      STRING NOT NULL,          -- approved | rejected | needs_changes
  comment       STRING,
  decided_at    TIMESTAMP NOT NULL,
  CONSTRAINT review_decisions_pk PRIMARY KEY (decision_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS deployments (
  deployment_id   STRING NOT NULL,
  artifact_id     STRING NOT NULL,
  target_env      STRING NOT NULL,        -- dev | staging | prod
  status          STRING NOT NULL,        -- pending | succeeded | failed
  deployed_by     STRING,
  deployed_at     TIMESTAMP NOT NULL,
  CONSTRAINT deployments_pk PRIMARY KEY (deployment_id)
) USING DELTA;
