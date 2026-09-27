#!/usr/bin/env python3
"""one.py -- Alteryx workflow -> Databricks Lakeflow SDP (PySpark), fully automated.

    python one.py <workflow.yxmd|.yxmc> [--catalog CAT] [--schema SCH]
                  [--outdir DIR] [--parse-candidates N] [--code-candidates N]
                  [--models-parse id1,id2,...] [--models-code id1,id2,...]
                  [--list-free-models]

Pipeline:
    1. extract     workflow XML -> structured JSON (extract_alteryx.js)
    2. flow-select run N free OpenRouter models on "identify the transformation
                   flow"; jev (TypeSafe) chooses the best one
    3. code-select run N free OpenRouter models on "generate the PySpark SDP
                   code" from the winning flow; each candidate is statically
                   validated against this repo's own target-code rules
                   (migration/engine/mig/targets/databricks_sdp.py) and jev
                   chooses the best one
    4. write       winning flow analysis, winning code, and a model-selection
                   report to --outdir

This is a fast, single-pass path for small/medium workflows. For workflows too
large for one context (hundreds-to-thousands of tools), use the `migration`
skill instead (`/migration:alteryx-sdp-migrate`), which this script's static
code-validation borrows from directly.
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

sys.path.insert(0, str(ROOT / "migration" / "engine"))
from mig.targets import databricks_sdp  # noqa: E402  (needs sys.path patch above)

OPENROUTER_KEY = os.environ.get("open_router") or os.environ.get("OPENROUTER_API_KEY")
JEV_KEY = os.environ.get("jev") or os.environ.get("TYPESAFE_API_KEY")
if not OPENROUTER_KEY:
    sys.exit("missing OpenRouter API key: set `open_router` in .env")
if not JEV_KEY:
    sys.exit("missing jev/TypeSafe API key: set `jev` in .env")

OPENROUTER_MODELS_URL = "https://openrouter.ai/api/v1/models"
OPENROUTER_CHAT_URL = "https://openrouter.ai/api/v1/chat/completions"
JEV_URL = "https://api.typesafe.ai/v1/systemone"
EXCLUDE_SUBSTRINGS = ("guard", "safety", "moderation")

PARSE_SYSTEM = """You are an expert Alteryx workflow analyst. You are given the exhaustive \
tool/connection inventory of an Alteryx workflow, extracted directly from its XML. Produce a \
precise transformation-flow analysis in Markdown:

1. Topological build order and logical stages (ingestion / cleansing / join-enrichment / \
business rules / aggregation / output).
2. For every ToolID in the inventory: what it does, its key configuration/expression, and its \
input/output field effects. Every ToolID must appear -- do not skip any.
3. Every filter condition, formula expression, join key, and aggregation, verbatim.
4. Determinism hazards: DateTimeNow/Today, RAND/RandInt, hard-coded paths, credentials, \
sort-order-dependent logic.
5. Any tool with no direct Spark/PySpark equivalent, flagged explicitly.

Be exhaustive and cite the configuration; never invent behavior that is not in the inventory."""

CODE_RULES_SUMMARY = """- PySpark DataFrame API only. No spark.sql(...), no .selectExpr(...), no \
F.expr(...) for logic. Only @dlt.expect_* predicates and spatial-library arguments may be raw \
strings.
- No F.current_date() / F.current_timestamp(). Every wall-clock reference is a `run_date` \
pipeline parameter.
- Every ordered/window operation has an explicit Window.orderBy with an explicit tiebreak column.
- Every .pivot() call takes an explicit value list, never inferred.
- Alteryx StdDev is population stddev -> use F.stddev_pop, not F.stddev.
- Never hardcode credentials or connection strings; reference named secret scopes.
- No .collect() / .toPandas() driver-side materialization inside a pipeline transform."""


def code_system(catalog: str, schema: str) -> str:
    return f"""You are an expert Databricks engineer. Given a transformation-flow analysis of a \
former Alteryx workflow, generate a complete Databricks Lakeflow Spark Declarative Pipeline \
(SDP/DLT) in PySpark implementing it end-to-end, structured as bronze/silver/gold \
@dlt.table / @dlt.view functions with @dlt.expect_* data-quality expectations drawn from the \
Alteryx filter/error-handling tools.

Hard rules (violating any of these disqualifies the output):
{CODE_RULES_SUMMARY}

Target Unity Catalog: catalog={catalog}, schema={schema}.

Output exactly one complete Python module and nothing else -- no explanation before or after."""


# --------------------------------------------------------------------------
# extract
# --------------------------------------------------------------------------

def run_extract(workflow_path: Path, outdir: Path) -> dict:
    if workflow_path.suffix.lower() not in (".yxmd", ".yxmc"):
        sys.exit(f"unsupported extension {workflow_path.suffix!r}; extract_alteryx.js handles "
                  ".yxmd/.yxmc only")
    node = shutil.which("node")
    if not node:
        sys.exit("node.js is required to run extract_alteryx.js (not found on PATH)")
    script = ROOT / "extract_alteryx.js"
    result = subprocess.run([node, str(script), str(workflow_path), "--outdir", str(outdir)],
                             capture_output=True, text=True, encoding="utf-8", errors="replace")
    if result.stdout.strip():
        print("    " + result.stdout.strip().replace("\n", "\n    "))
    if result.returncode != 0:
        sys.exit(f"extract_alteryx.js failed:\n{result.stderr}")
    out_name = re.sub(r"\.(yxmd|yxmc)$", "", workflow_path.name, flags=re.I) + ".extracted.json"
    out_path = outdir / out_name
    if not out_path.is_file():
        sys.exit(f"expected extracted JSON at {out_path} but it was not created")
    return json.loads(out_path.read_text(encoding="utf-8"))


def build_digest(extracted: dict) -> str:
    nodes = extracted.get("nodes", [])
    budget_chars = 45_000
    per_node_cap = max(150, budget_chars // max(1, len(nodes)))

    L = []
    a = L.append
    a(f"# Workflow: {extracted.get('fileName')}")
    a(f"is_macro: {extracted.get('isMacro')}")
    a(f"meta: {json.dumps(extracted.get('workflowMeta') or {}, ensure_ascii=False)}")
    a(f"totals: {json.dumps(extracted.get('totals') or {}, ensure_ascii=False)}")

    if extracted.get("containers"):
        a("\n## Containers")
        for c in extracted["containers"]:
            a(f"- {c['toolId']} `{c['caption']}` under {c['containerPath']}")

    if extracted.get("macros"):
        a("\n## Macros referenced (not recursively expanded in this pass)")
        for m in extracted["macros"]:
            a(f"- ToolID {m['toolId']} -> {m['macroFile']} ({m.get('annotation') or ''})")

    if extracted.get("connections"):
        a("\n## Connections")
        for c in extracted["connections"]:
            a(f"- {c['originToolId']}:{c['originConnection']} -> "
              f"{c['destToolId']}:{c['destConnection']}")

    a("\n## Nodes")
    for n in nodes:
        head = f"### ToolID {n['toolId']} -- {n.get('plugin')}"
        a("\n" + head)
        a(f"- container: {n.get('containerPath')}")
        if n.get("annotation"):
            a(f"- annotation: {n['annotation']}")
        if n.get("isMacroCall"):
            a(f"- macro call: {n.get('macroFile')}")
        cfg = n.get("configRaw") or ""
        if len(cfg) > per_node_cap:
            cfg = cfg[:per_node_cap] + f"...[truncated, {len(cfg) - per_node_cap} more chars]"
        if cfg:
            a(f"- config: {cfg}")

    digest = "\n".join(L)
    if len(nodes) > 350:
        print(f"    ! {len(nodes)} tools is large for a single-pass script; consider "
              "`/migration:alteryx-sdp-migrate` for an exhaustive, resumable run", file=sys.stderr)
    return digest


# --------------------------------------------------------------------------
# OpenRouter (free models)
# --------------------------------------------------------------------------

def fetch_free_models() -> list[dict]:
    r = requests.get(OPENROUTER_MODELS_URL, timeout=30)
    r.raise_for_status()
    data = r.json()["data"]
    return [m for m in data if m.get("id", "").endswith(":free")
            and not any(b in m["id"].lower() for b in EXCLUDE_SUBSTRINGS)]


def rank_candidates(models: list[dict], task: str) -> list[str]:
    """Full ranked pool (not sliced) so a caller can walk past models that turn
    out to be gated/unavailable at call time and still reach `n` successes."""
    pool = list(models)
    if task == "code":
        pool.sort(key=lambda m: (0 if "code" in m["id"].lower() else 1,
                                  -(m.get("context_length") or 0)))
    else:
        pool.sort(key=lambda m: -(m.get("context_length") or 0))
    if not pool:
        sys.exit("no free OpenRouter models currently available")
    return [m["id"] for m in pool]


def run_candidates(ranked_ids: list[str], want_n: int, call_fn):
    """Walk the ranked pool calling call_fn(model_id) -> str, keeping the first
    `want_n` successes. A model that 403s (agentic-harness gating, provider
    policy) or otherwise fails is skipped rather than aborting the whole task --
    free-tier availability is flaky enough that this matters in practice."""
    outputs, tried, failed = {}, [], {}
    for model_id in ranked_ids:
        if len(outputs) >= want_n:
            break
        tried.append(model_id)
        try:
            outputs[model_id] = call_fn(model_id)
        except Exception as e:
            failed[model_id] = str(e)
            print(f"    ! {model_id} failed: {e}")
    return outputs, tried, failed


def call_openrouter(model: str, system: str, user: str, max_tokens: int = 6000,
                     timeout: int = 180) -> str:
    headers = {
        "Authorization": f"Bearer {OPENROUTER_KEY}",
        "Content-Type": "application/json",
        "X-Title": "alteryx-extractor/one.py",
    }
    body = {
        "model": model,
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "temperature": 0.2,
        "max_tokens": max_tokens,
    }
    last_err = None
    for attempt in range(3):
        try:
            r = requests.post(OPENROUTER_CHAT_URL, headers=headers, json=body, timeout=timeout)
        except requests.RequestException as e:
            last_err = e
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 429:
            time.sleep(2 ** attempt * 2)
            continue
        r.raise_for_status()
        data = r.json()
        if "choices" not in data:
            raise RuntimeError(f"{model}: unexpected response {data}")
        content = data["choices"][0]["message"]["content"]
        if not content:
            finish_reason = data["choices"][0].get("finish_reason")
            raise RuntimeError(f"{model}: empty response (finish_reason={finish_reason})")
        return content
    raise RuntimeError(f"{model}: failed after retries ({last_err})")


_FENCE = re.compile(r"```(?:python)?\s*\n(.*?)```", re.S)


def strip_code_fence(text: str) -> str:
    text = text.strip()
    m = _FENCE.search(text)
    return m.group(1).strip() if m else text


# --------------------------------------------------------------------------
# jev (TypeSafe) -- structured judgment used to pick the winning candidate
# --------------------------------------------------------------------------

def call_jev(state, questions: dict, model: str = "jev-latest", timeout: int = 60) -> dict:
    headers = {"Authorization": f"Bearer {JEV_KEY}", "Content-Type": "application/json"}
    body = {"state": state, "model": model, "questions": questions}
    last_err = None
    for attempt in range(4):
        try:
            r = requests.post(JEV_URL, headers=headers, json=body, timeout=timeout)
        except requests.RequestException as e:
            last_err = e
            time.sleep(2 ** attempt)
            continue
        if r.status_code in (429, 529):
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"jev: failed after retries ({last_err})")


def judge_flow(candidates: dict[str, str], per_candidate_chars: int = 3000):
    if len(candidates) == 1:
        only = next(iter(candidates))
        return only, {"note": "only one candidate produced output; jev not consulted"}
    state = {
        "task": "alteryx-transformation-flow-identification",
        "what_was_asked_of_each_model": PARSE_SYSTEM,
        "candidates": {
            mid: txt[:per_candidate_chars] + ("...[truncated]" if len(txt) > per_candidate_chars
                                               else "")
            for mid, txt in candidates.items()
        },
    }
    questions = {
        "winner": {
            "type": "choice",
            "instructions": "Which candidate's transformation-flow analysis is most complete "
                            "(covers every ToolID), correct, and structurally sound? Judge only "
                            "the text under state.candidates.",
            "criteria": {mid: f"the analysis attributed to {mid}" for mid in candidates},
        }
    }
    result = call_jev(state, questions)
    return result["answers"]["winner"]["choice"], result


def judge_code(candidates: dict[str, str], issues: dict[str, list[str]],
                per_candidate_chars: int = 4000):
    if len(candidates) == 1:
        only = next(iter(candidates))
        return only, {"note": "only one candidate available; jev not consulted",
                       "static_issues": issues}
    state = {
        "task": "pyspark-sdp-code-generation",
        "target_code_rules": CODE_RULES_SUMMARY,
        "static_validation_issues": {mid: issues.get(mid, []) for mid in candidates},
        "candidates": {
            mid: txt[:per_candidate_chars] + ("...[truncated]" if len(txt) > per_candidate_chars
                                               else "")
            for mid, txt in candidates.items()
        },
    }
    questions = {
        "winner": {
            "type": "choice",
            "instructions": "Which candidate's PySpark SDP code is most correct and complete, "
                            "and best complies with target_code_rules? Weigh "
                            "static_validation_issues heavily -- a candidate with issues should "
                            "lose to a clean one unless every candidate has issues.",
            "criteria": {mid: f"the code attributed to {mid}" for mid in candidates},
        }
    }
    result = call_jev(state, questions)
    return result["answers"]["winner"]["choice"], result


# --------------------------------------------------------------------------
# static validation -- reuses this repo's own target-code rules
# --------------------------------------------------------------------------

def static_check_code(src: str) -> list[str]:
    try:
        tree = ast.parse(src)
    except SyntaxError as e:
        return [f"syntax error at line {e.lineno}: {e.msg}"]

    issues = []
    for check, pattern, why in databricks_sdp.forbidden:
        for m in pattern.finditer(src):
            line = src[:m.start()].count("\n") + 1
            issues.append(f"{check} at line {line}: {why}")

    findings = []
    ctx = SimpleNamespace(tree=tree, src=src, label="candidate",
                          record=lambda c, s, d: findings.append((c, s, d)))
    databricks_sdp.code_checks(ctx)
    issues.extend(f"{c}: {d}" for c, s, d in findings if s == "fail")
    return issues


# --------------------------------------------------------------------------

def safe(model_id: str) -> str:
    return model_id.replace("/", "_").replace(":", "_")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("workflow", nargs="?", help="path to a .yxmd or .yxmc file")
    ap.add_argument("--outdir", help="output directory (default: <workflow>_sdp_output/)")
    ap.add_argument("--catalog", default="main", help="target Unity Catalog catalog")
    ap.add_argument("--schema", default="migrated", help="target Unity Catalog schema")
    ap.add_argument("--parse-candidates", type=int, default=3)
    ap.add_argument("--code-candidates", type=int, default=3)
    ap.add_argument("--models-parse", help="comma-separated OpenRouter model ids, overrides auto-pick")
    ap.add_argument("--models-code", help="comma-separated OpenRouter model ids, overrides auto-pick")
    ap.add_argument("--list-free-models", action="store_true",
                     help="print currently-free OpenRouter models and exit")
    args = ap.parse_args()

    if args.list_free_models:
        for m in fetch_free_models():
            print(f"{m['id']:55s} ctx={m.get('context_length')}")
        return

    if not args.workflow:
        ap.error("workflow path is required unless --list-free-models is given")

    workflow_path = Path(args.workflow).resolve()
    if not workflow_path.is_file():
        sys.exit(f"no such file: {workflow_path}")

    outdir = Path(args.outdir).resolve() if args.outdir else \
        workflow_path.parent / f"{workflow_path.stem}_sdp_output"
    outdir.mkdir(parents=True, exist_ok=True)
    candidates_dir = outdir / "candidates"
    candidates_dir.mkdir(exist_ok=True)

    print(f"[1/5] Extracting {workflow_path.name} ...")
    extracted = run_extract(workflow_path, outdir)
    digest = build_digest(extracted)
    (outdir / f"{workflow_path.stem}.digest.md").write_text(digest, encoding="utf-8")

    models = fetch_free_models()

    parse_ranked = ([m.strip() for m in args.models_parse.split(",")] if args.models_parse
                    else rank_candidates(models, "parse"))
    print(f"[2/5] Identifying transformation flow (want {args.parse_candidates} of "
          f"{len(parse_ranked)} ranked free model(s)) ...")

    def _parse_call(m):
        out = call_openrouter(m, PARSE_SYSTEM, digest, max_tokens=6000)
        (candidates_dir / f"parse__{safe(m)}.md").write_text(out, encoding="utf-8")
        return out

    parse_outputs, parse_tried, parse_failed = run_candidates(
        parse_ranked, args.parse_candidates, _parse_call)
    if not parse_outputs:
        sys.exit("no candidate model produced a transformation-flow analysis; aborting")
    print(f"    succeeded: {', '.join(parse_outputs)}")

    print("[3/5] Asking jev to pick the best transformation-flow analysis ...")
    flow_winner, flow_verdict = judge_flow(parse_outputs)
    print(f"    winner: {flow_winner}")
    flow_text = parse_outputs[flow_winner]
    (outdir / f"{workflow_path.stem}.transformation-flow.md").write_text(flow_text,
                                                                          encoding="utf-8")

    code_ranked = ([m.strip() for m in args.models_code.split(",")] if args.models_code
                   else rank_candidates(models, "code"))
    print(f"[4/5] Generating PySpark SDP code (want {args.code_candidates} of "
          f"{len(code_ranked)} ranked free model(s)) ...")
    system = code_system(args.catalog, args.schema)
    user = (f"Transformation-flow analysis:\n\n{flow_text}\n\n"
            "Generate the complete PySpark SDP module now.")
    code_issues = {}

    def _code_call(m):
        raw = call_openrouter(m, system, user, max_tokens=8000)
        src = strip_code_fence(raw)
        code_issues[m] = static_check_code(src)
        (candidates_dir / f"code__{safe(m)}.py").write_text(src, encoding="utf-8")
        if code_issues[m]:
            print(f"    ! {m}: {len(code_issues[m])} static issue(s)")
        return src

    code_outputs, code_models, code_failed = run_candidates(
        code_ranked, args.code_candidates, _code_call)
    if not code_outputs:
        sys.exit("no candidate model produced PySpark code; aborting")
    print(f"    succeeded: {', '.join(code_outputs)}")

    passing = {m: t for m, t in code_outputs.items() if not code_issues[m]}
    pool = passing if passing else code_outputs
    if not passing:
        print("    ! no candidate passed static validation cleanly; judging among all anyway")

    print("[5/5] Asking jev to pick the best PySpark SDP code ...")
    code_winner, code_verdict = judge_code(pool, code_issues)
    print(f"    winner: {code_winner}")
    winning_code = code_outputs[code_winner]
    (outdir / f"{workflow_path.stem}_sdp.py").write_text(winning_code, encoding="utf-8")

    report = {
        "workflow": str(workflow_path),
        "node_count": len(extracted.get("nodes", [])),
        "transformation_flow": {
            "candidates_tried": parse_tried,
            "candidates_succeeded": list(parse_outputs),
            "candidates_failed": parse_failed,
            "winner": flow_winner,
            "jev_verdict": flow_verdict,
        },
        "pyspark_sdp_code": {
            "candidates_tried": code_models,
            "candidates_succeeded": list(code_outputs),
            "candidates_failed": code_failed,
            "static_issues": code_issues,
            "winner": code_winner,
            "jev_verdict": code_verdict,
        },
        "outputs": {
            "digest": str(outdir / f"{workflow_path.stem}.digest.md"),
            "transformation_flow": str(outdir / f"{workflow_path.stem}.transformation-flow.md"),
            "sdp_code": str(outdir / f"{workflow_path.stem}_sdp.py"),
            "candidates_dir": str(candidates_dir),
        },
    }
    (outdir / f"{workflow_path.stem}.model-selection.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8")

    print(f"\nDone. Outputs in {outdir}")


if __name__ == "__main__":
    main()
