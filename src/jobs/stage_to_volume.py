# Databricks notebook source
# Phase 1, ingest job — task 1: copy uploaded source files into a UC Volume.
# Paste this as a notebook under Workspace, or leave it in the bundle and let
# `databricks bundle deploy` sync it — the jobs.yml notebook_task path points here.

dbutils.widgets.text("catalog", "migration_platform")
dbutils.widgets.text("schema", "core")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

volume_path = f"/Volumes/{catalog}/{schema}/staged_sources"
dbutils.fs.mkdirs(volume_path)

# TODO: replace with real ingestion (app upload landing dir -> volume_path).
print(f"Staging area ready at {volume_path}")
