"""Migration Platform review app (Phase 1). Streamlit, deployed via
`databricks apps deploy migration-platform --source-code-path ./app`.

No transformation logic lives here — the app only moves files into the
Volume, writes/reads the six UC tables, and (for the manual Phase-1 loop)
runs already-approved SQL directly. The actual analyze/transpile/AI work
happens in the Lakeflow Jobs (see src/jobs/).
"""

import os
import uuid
from datetime import datetime, timezone

import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

st.set_page_config(page_title="Migration Platform", layout="wide")

CATALOG = os.environ.get("MIGRATION_CATALOG", "migration_platform")
SCHEMA = os.environ.get("MIGRATION_SCHEMA", "core")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/staged_sources"
INGEST_JOB_NAME = "migration_platform_ingest_and_profile"


@st.cache_resource
def get_connection():
    cfg = Config()  # picks up the app's attached SQL warehouse via env vars
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: cfg.authenticate,
    )


@st.cache_resource
def get_workspace_client():
    return WorkspaceClient()


def run_query(query: str, params: tuple = ()):
    """SELECT — returns a pandas DataFrame."""
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.fetchall_arrow().to_pandas()


def execute(statement: str, params: tuple = ()):
    """INSERT/UPDATE/DDL or arbitrary generated SQL — no result set expected."""
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(statement, params)


def current_user() -> str:
    """Databricks Apps forwards the viewer's identity as request headers."""
    try:
        headers = st.context.headers
        return headers.get("X-Forwarded-Email") or headers.get("X-Forwarded-User") or "unknown"
    except Exception:
        return "unknown"


def trigger_ingest_job():
    """Best-effort: kick the ingest_and_profile job so the uploaded file
    gets analyzed without the user needing to go find it in Workflows."""
    try:
        w = get_workspace_client()
        jobs = list(w.jobs.list(name=INGEST_JOB_NAME))
        if jobs:
            w.jobs.run_now(job_id=jobs[0].job_id)
            return True
    except Exception as e:
        st.warning(f"Could not auto-trigger ingest job — run it manually from Workflows. ({e})")
    return False


st.title("Migration Platform — Review Queue")

tab_upload, tab_review, tab_deploy = st.tabs(["Upload", "Review", "Deploy"])

# ---------------------------------------------------------------- Upload ---
with tab_upload:
    source_system = st.selectbox("Source system", ["oracle", "alteryx", "pentaho"])
    object_type = st.selectbox("Object type", ["stored_procedure", "workflow", "pipeline"])
    files = st.file_uploader("Upload source object(s)", accept_multiple_files=True)

    if st.button("Stage for migration", disabled=not files):
        w = get_workspace_client()
        staged = 0
        for f in files:
            raw_path = f"{VOLUME_PATH}/{f.name}"
            w.files.upload(raw_path, f.getvalue(), overwrite=True)
            execute(
                f"""INSERT INTO {CATALOG}.{SCHEMA}.source_objects
                    (object_id, source_system, object_type, object_name, raw_path, ingested_at)
                    VALUES (%s, %s, %s, %s, %s, %s)""",
                (str(uuid.uuid4()), source_system, object_type, f.name, raw_path, datetime.now(timezone.utc)),
            )
            staged += 1
        st.success(f"Staged {staged} file(s) to {VOLUME_PATH}")
        if trigger_ingest_job():
            st.info("Triggered the ingest_and_profile job — check Workflows for progress.")

# ---------------------------------------------------------------- Review ---
with tab_review:
    df = run_query(
        f"""
        SELECT ga.artifact_id, so.object_name, so.source_system, ga.target_shape,
               ga.confidence_note,
               EXISTS (
                 SELECT 1 FROM {CATALOG}.{SCHEMA}.validation_results vr
                 WHERE vr.artifact_id = ga.artifact_id AND vr.passed
               ) AS reconciled
        FROM {CATALOG}.{SCHEMA}.generated_artifacts ga
        JOIN {CATALOG}.{SCHEMA}.source_objects so ON so.object_id = ga.object_id
        LEFT JOIN {CATALOG}.{SCHEMA}.review_decisions rd ON rd.artifact_id = ga.artifact_id
        WHERE rd.artifact_id IS NULL
        """
    )
    st.dataframe(df, use_container_width=True)

    selected = st.selectbox("Artifact to review", df["artifact_id"] if not df.empty else [])
    if selected:
        row = df.loc[df.artifact_id == selected].iloc[0]
        detail = run_query(
            f"SELECT code FROM {CATALOG}.{SCHEMA}.generated_artifacts WHERE artifact_id = %s",
            (selected,),
        )
        code = detail.iloc[0]["code"] if not detail.empty else ""
        if row["confidence_note"]:
            st.info(f"AI review note: {row['confidence_note']}")
        if not row["reconciled"]:
            # Phase 1: reconcile job isn't built yet, so this is a warning, not
            # a hard gate. Once Phase 3 lands, change this back to a hard
            # `disabled=not row["reconciled"]` on the Approve button below.
            st.warning("No passing reconciliation result yet (reconcile job is Phase 3) — approving on review alone.")
        st.code(code, language="sql" if row["target_shape"] == "sql" else "python")

        comment = st.text_input("Comment (optional)")
        col1, col2 = st.columns(2)
        if col1.button("Approve"):
            execute(
                f"""INSERT INTO {CATALOG}.{SCHEMA}.review_decisions
                    (decision_id, artifact_id, reviewer, decision, comment, decided_at)
                    VALUES (%s, %s, %s, 'approved', %s, %s)""",
                (str(uuid.uuid4()), selected, current_user(), comment, datetime.now(timezone.utc)),
            )
            st.success("Approved.")
            st.rerun()
        if col2.button("Reject"):
            execute(
                f"""INSERT INTO {CATALOG}.{SCHEMA}.review_decisions
                    (decision_id, artifact_id, reviewer, decision, comment, decided_at)
                    VALUES (%s, %s, %s, 'rejected', %s, %s)""",
                (str(uuid.uuid4()), selected, current_user(), comment, datetime.now(timezone.utc)),
            )
            st.success("Rejected.")
            st.rerun()

# ---------------------------------------------------------------- Deploy ---
with tab_deploy:
    st.write("Approved artifacts pending deployment.")
    pending = run_query(
        f"""
        SELECT ga.artifact_id, so.object_name, ga.target_shape, ga.code
        FROM {CATALOG}.{SCHEMA}.generated_artifacts ga
        JOIN {CATALOG}.{SCHEMA}.source_objects so ON so.object_id = ga.object_id
        JOIN {CATALOG}.{SCHEMA}.review_decisions rd
          ON rd.artifact_id = ga.artifact_id AND rd.decision = 'approved'
        WHERE NOT EXISTS (
          SELECT 1 FROM {CATALOG}.{SCHEMA}.deployments d
          WHERE d.artifact_id = ga.artifact_id AND d.status = 'succeeded'
        )
        """
    )
    st.dataframe(pending[["artifact_id", "object_name", "target_shape"]], use_container_width=True)

    to_run = st.selectbox("Artifact to run", pending["artifact_id"] if not pending.empty else [])
    if to_run:
        target_row = pending.loc[pending.artifact_id == to_run].iloc[0]
        # Phase 1 step 7: run the generated SQL as a manual one-off — no bundle
        # deploy yet. PySpark/pipeline shapes still need a job trigger (TODO).
        if target_row["target_shape"] == "sql":
            if st.button("Run now"):
                deployment_id = str(uuid.uuid4())
                try:
                    execute(target_row["code"])
                    execute(
                        f"""INSERT INTO {CATALOG}.{SCHEMA}.deployments
                            (deployment_id, artifact_id, target_env, status, deployed_by, deployed_at)
                            VALUES (%s, %s, 'dev', 'succeeded', %s, %s)""",
                        (deployment_id, to_run, current_user(), datetime.now(timezone.utc)),
                    )
                    st.success("Ran successfully and recorded as deployed.")
                except Exception as e:
                    execute(
                        f"""INSERT INTO {CATALOG}.{SCHEMA}.deployments
                            (deployment_id, artifact_id, target_env, status, deployed_by, deployed_at)
                            VALUES (%s, %s, 'dev', 'failed', %s, %s)""",
                        (deployment_id, to_run, current_user(), datetime.now(timezone.utc)),
                    )
                    st.error(f"Run failed: {e}")
        else:
            st.caption(f"'{target_row['target_shape']}' artifacts deploy via a job trigger — not wired up yet (Phase 2+).")
