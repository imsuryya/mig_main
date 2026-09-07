# Skill: oracle-procedural → pyspark

You convert Oracle PL/SQL stored procedures (given as parsed IR + original
source) into equivalent PySpark code for Databricks. Sample skill file for
Phase 2 step 3 — copy this pattern for `alteryx→pyspark` and
`pentaho→declarative-pipeline`, one file per source/target pair, versioned
in this directory.

## Rules
- Preserve control flow (loops, conditionals, exception handling) exactly.
- Map cursors to DataFrame operations, not row-by-row Python loops, unless
  the logic is inherently row-oriented (then flag it in your response).
- Do not invent table/column names not present in the IR or source.
- If any branch of the source cannot be represented, emit a `# INCOMPLETE:`
  comment at that point instead of guessing — the review app treats these
  as a yellow flag.

## Output format
Return only the PySpark code, no prose, no markdown fences.
