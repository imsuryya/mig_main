# Migration skills

Two conventions live side by side here.

## `migration/` — the `*-to-sdp` family (preferred going forward)

```
migration/
├── plugin.json
├── references/                     # SHARED — Databricks/SDP target, reused by every skill
│   ├── databricks-sdp-target.md    #   Lakeflow/DLT PySpark API patterns, medallion mapping, CDC, DAB
│   ├── sdp-production.md           #   hardening: determinism, expectations, params/secrets, testing
│   └── sdp-optimization.md         #   performance: ingest-once, broadcast, pivots, windows, storage
└── skills/
    └── alteryx-to-sdp/             # source-specific
        ├── SKILL.md
        ├── references/             #   Alteryx-only: workflow XML model, formula syntax, tool reference
        ├── examples/                #   a worked example plan
        └── scripts/                 #   optional PowerShell for golden-output parity (needs local Alteryx)
```

`migration/references/` describes the **target** (Databricks SDP/DLT) and is
identical for every source tool — never duplicate it per-skill. Each
`migration/skills/<source>-to-sdp/references/` holds only what's specific to
parsing *that* source format.

### Adding the next one (Oracle, Pentaho, SSIS, Informatica, …)

1. `mkdir -p migration/skills/<source>-to-sdp/{references,examples}`
2. Write `SKILL.md` with the same phase structure as `alteryx-to-sdp/SKILL.md`
   (exhaustive parse → semantic model/lineage → target SDP architecture →
   tool-by-tool map → DQ/parity/testing → deliverable), and point at the
   shared docs the same way: `../../references/databricks-sdp-target.md`.
3. Put source-specific parsing/translation reference docs under that skill's
   own `references/`.

This family is a **planning** skill — its deliverable is a written migration
plan (markdown), with code generated as a follow-up. It's built for
interactive/agentic use (an agent reading a real source file and writing
plan documents), not the single-shot completion calls `src/jobs/ai_assist.py`
makes. `ai_assist.py` can resolve it (`migration/skills/<source>-to-sdp/SKILL.md`)
for completeness, but expect it to behave like a detailed design brief rather
than a drop-in code generator in that automated context.

## Flat single-file skills (older convention)

`oracle-procedural-pyspark.md` — a terse, code-only completion skill, written
to fit `ai_assist.py`'s one-shot "fill in this incomplete snippet" call
directly. Keep new source/target pairs that are meant for that automated
loop in this flat style (`<source>-<object_type>-<target_shape>.md`), and use
`migration/skills/<source>-to-sdp/` for anything meant to produce a full
migration plan instead.
