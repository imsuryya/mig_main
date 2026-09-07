---
name: alteryx-pyspark
description: Convert an Alteryx Designer workflow (.yxmd/.yxmc) into equivalent PySpark. Phase 2 step 3 skill for source_system=alteryx.
---

# Skill: alteryx → pyspark

You convert Alteryx Designer workflow XML (given as parsed IR + the original
`.yxmd`/`.yxmc` source) into equivalent PySpark code for Databricks.

Two reference docs ship alongside this skill and are loaded into context
before every call — read them before generating code:

- `references/alteryx-designer/workflow-xml.md` — the `.yxmd`/`.yxmc` XML
  structure: `Node`/`Connection`/`Properties`, container semantics, anchor
  names (`Input`, `Output`, `True`/`False`, `Left`/`Right`/`Join`), and how
  to trace the graph from `ToolID` references.
- `references/alteryx-designer/formula-syntax.md` — the full Alteryx
  Formula tool function/operator reference, needed to translate `Formula`,
  `Filter`, and `Multi-Field Formula` tool expressions into PySpark
  column expressions.

## Mapping rules

- Walk the workflow graph via root `Connections`, not visual layout —
  `Origin ToolID` → `Destination ToolID`, following anchor names exactly
  (a `True`/`False` pair from a Filter tool becomes two DataFrame branches,
  not one).
- Map common tools directly:
  - `Input Data` / `Output Data` → `spark.read` / `df.write` (infer format
    from the tool's file-type config: csv, xlsx via a suitable reader,
    parquet, database connection string).
  - `Select` → `.select()` / `.drop()` / column rename via `.withColumnRenamed()`.
  - `Filter` → `.filter()` for the `True` branch, `~condition` for `False`.
  - `Formula` / `Multi-Field Formula` → `.withColumn()`, translating each
    Alteryx formula expression per `formula-syntax.md`'s function reference
    (e.g. `IIF(bool,x,y)` → `F.when(bool, x).otherwise(y)`, `Contains` →
    `F.col(...).contains(...)`, null semantics per that doc's Null Handling
    table — Alteryx's `Null() == Null()` is `True`, unlike SQL/Spark's
    three-valued logic, so wrap equality checks accordingly).
  - `Join` → `.join()`, mapping `Left`/`Right`/`Join` anchors to left/right
    unmatched/inner outputs respectively.
  - `Union` → `.unionByName(allowMissingColumns=True)`.
  - `Summarize` → `.groupBy().agg()`.
  - `Sort` → `.orderBy()`.
  - `Unique` → `.dropDuplicates()`.
- `ToolContainer`/`ControlContainer` nodes group tools visually only —
  flatten them; do not create a separate DataFrame scope per container.
- A macro (`.yxmc`) or analytic app (`.yxwz`) becomes a PySpark function
  taking its `MacroInputs`/interface `Questions` as parameters — do not
  inline it into the caller.
- If a tool has no direct PySpark equivalent (spatial tools, R/Python tool
  with custom code, a proprietary connector), do not invent behavior: emit
  `# INCOMPLETE: <ToolID> <tool name> — <why>` at that point instead of
  guessing. The review app treats this as a yellow flag for AI-assist.
- Preserve the source's field names and types where Alteryx's `MetaInfo`
  cached schema states them explicitly.

## Output format

Return only the PySpark code, no prose, no markdown fences.
