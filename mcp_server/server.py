"""Migration Platform MCP server (Phase 3). Wraps the same backend functions
the review app calls, deployed as a second Databricks App:
  databricks apps deploy migration-platform-mcp --source-code-path ./mcp_server

Connect Claude Code / Claude Desktop to the deployed app URL as an MCP
endpoint to drive migrations end-to-end from chat.
"""

import os

from databricks import sql
from databricks.sdk.core import Config
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("migration-platform")

CATALOG = os.environ.get("MIGRATION_CATALOG", "migration_platform")
SCHEMA = os.environ.get("MIGRATION_SCHEMA", "core")


def _connection():
    cfg = Config()
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: cfg.authenticate,
    )


@mcp.tool()
def list_objects(status: str = "pending_review") -> list[dict]:
    """List migrated objects filtered by status (pending_review, approved, deployed)."""
    with _connection().cursor() as cur:
        cur.execute(
            f"""
            SELECT ga.artifact_id, so.object_name, so.source_system, ga.target_shape
            FROM {CATALOG}.{SCHEMA}.generated_artifacts ga
            JOIN {CATALOG}.{SCHEMA}.source_objects so ON so.object_id = ga.object_id
            LEFT JOIN {CATALOG}.{SCHEMA}.review_decisions rd ON rd.artifact_id = ga.artifact_id
            WHERE (%s = 'pending_review' AND rd.artifact_id IS NULL)
               OR (%s != 'pending_review' AND rd.decision = %s)
            """,
            (status, status, status),
        )
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


@mcp.tool()
def get_diff(artifact_id: str) -> dict:
    """Return source code and generated code for one artifact."""
    with _connection().cursor() as cur:
        cur.execute(
            f"""
            SELECT so.raw_path, ga.code, ga.confidence_note
            FROM {CATALOG}.{SCHEMA}.generated_artifacts ga
            JOIN {CATALOG}.{SCHEMA}.source_objects so ON so.object_id = ga.object_id
            WHERE ga.artifact_id = %s
            """,
            (artifact_id,),
        )
        row = cur.fetchone()
        return {"source_path": row[0], "generated_code": row[1], "confidence_note": row[2]} if row else {}


@mcp.tool()
def approve(artifact_id: str, reviewer: str, comment: str = "") -> str:
    """Approve an artifact for deployment (blocked unless it has a passing reconcile row)."""
    with _connection().cursor() as cur:
        cur.execute(
            f"""SELECT 1 FROM {CATALOG}.{SCHEMA}.validation_results
                WHERE artifact_id = %s AND passed""",
            (artifact_id,),
        )
        if not cur.fetchone():
            return "blocked: no passing validation_results row for this artifact"
        cur.execute(
            f"""INSERT INTO {CATALOG}.{SCHEMA}.review_decisions
                (decision_id, artifact_id, reviewer, decision, comment, decided_at)
                VALUES (uuid(), %s, %s, 'approved', %s, current_timestamp())""",
            (artifact_id, reviewer, comment),
        )
        return "approved"


@mcp.tool()
def deploy(artifact_id: str, target_env: str, deployed_by: str) -> str:
    """Record a deployment. TODO: trigger the actual bundle/job deploy."""
    with _connection().cursor() as cur:
        cur.execute(
            f"""INSERT INTO {CATALOG}.{SCHEMA}.deployments
                (deployment_id, artifact_id, target_env, status, deployed_by, deployed_at)
                VALUES (uuid(), %s, %s, 'pending', %s, current_timestamp())""",
            (artifact_id, target_env, deployed_by),
        )
        return f"deployment queued for {target_env}"


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
