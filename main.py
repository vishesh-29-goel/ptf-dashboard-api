"""
PTF Dashboard API — FastAPI backend for the PTF L1 Sanctions Screening Dashboard.

Reads live from the agent-managed Postgres DB (ptf_screening_results_v2,
ptf_payment_messages_v2, ptf_scenario_groups) and returns structured JSON
for the dashboard frontend.

Environment variables required:
  DATABASE_URL   — Postgres connection string (agent-managed DB)

Run locally:
  uvicorn main:app --reload --port 8000

Deploy on Railway:
  Set DATABASE_URL in Railway environment variables.
"""

import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware


# ── DB connection pool (simple — one conn per process, Railway keeps it alive) ──

_conn = None


def get_db():
    global _conn
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    # Reconnect if connection dropped
    try:
        if _conn is None or _conn.closed:
            _conn = psycopg2.connect(db_url)
            _conn.autocommit = True
        else:
            _conn.cursor().execute("SELECT 1")  # ping
    except Exception:
        _conn = psycopg2.connect(db_url)
        _conn.autocommit = True
    return _conn


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_db()  # warm up connection on startup
    yield


# ── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="PTF Dashboard API",
    description="Live backend for the PTF L1 Sanctions Screening Dashboard",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def rows_to_dicts(cur) -> list[dict]:
    cols = [d[0] for d in cur.description]
    result = []
    for row in cur.fetchall():
        d = {}
        for c, v in zip(cols, row):
            if isinstance(v, datetime):
                d[c] = v.isoformat()
            else:
                d[c] = v
        result.append(d)
    return result


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/stats")
def get_stats():
    """Overall summary stats across all screening results."""
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (sr.payment_id)
                sr.payment_id,
                sr.final_decision,
                sr.elapsed_time_ms,
                sr.created_at,
                pm.scenario_group_id
            FROM ptf_screening_results_v2 sr
            JOIN ptf_payment_messages_v2 pm ON pm.id = sr.payment_id
            ORDER BY sr.payment_id, sr.id DESC
        )
        SELECT
            COUNT(*)                                                         AS total_screened,
            COUNT(*) FILTER (WHERE final_decision = 'PASS_L1')              AS passed,
            COUNT(*) FILTER (WHERE final_decision = 'PEND_L2')              AS pend_l2,
            COUNT(*) FILTER (WHERE final_decision = 'PEND_L1')              AS pend_l1,
            COUNT(DISTINCT scenario_group_id)                               AS total_groups,
            ROUND(AVG(elapsed_time_ms)::numeric, 0)                         AS avg_elapsed_ms,
            MAX(created_at)                                                  AS last_run_at
        FROM latest
    """)
    row = cur.fetchone()
    cols = [d[0] for d in cur.description]
    stats = dict(zip(cols, row))
    if isinstance(stats.get("last_run_at"), datetime):
        stats["last_run_at"] = stats["last_run_at"].isoformat()

    # Accuracy: correct = (is_true_positive=1 → PEND_L2) or (is_true_positive=0 → PASS_L1)
    cur.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (sr.payment_id)
                sr.final_decision,
                pm.is_true_positive
            FROM ptf_screening_results_v2 sr
            JOIN ptf_payment_messages_v2 pm ON pm.id = sr.payment_id
            ORDER BY sr.payment_id, sr.id DESC
        )
        SELECT
            COUNT(*)                                        AS total,
            COUNT(*) FILTER (
                WHERE (is_true_positive = 1 AND final_decision = 'PEND_L2')
                   OR (is_true_positive = 0 AND final_decision = 'PASS_L1')
            )                                               AS correct
        FROM latest
    """)
    acc_row = cur.fetchone()
    if acc_row and acc_row[0]:
        stats["accuracy_pct"] = round(acc_row[1] / acc_row[0] * 100, 1)
        stats["correct"] = acc_row[1]
    else:
        stats["accuracy_pct"] = None
        stats["correct"] = None

    cur.close()
    return stats


@app.get("/api/groups")
def get_groups():
    """List all scenario groups with alert + result counts."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (sr.payment_id)
                sr.payment_id,
                sr.final_decision,
                sr.created_at
            FROM ptf_screening_results_v2 sr
            ORDER BY sr.payment_id, sr.id DESC
        )
        SELECT
            sg.id,
            sg.group_name,
            sg.description,
            COUNT(DISTINCT pm.id)                                               AS alert_count,
            COUNT(DISTINCT l.payment_id)                                        AS result_count,
            COUNT(DISTINCT l.payment_id) FILTER (WHERE l.final_decision = 'PASS_L1') AS passed,
            COUNT(DISTINCT l.payment_id) FILTER (WHERE l.final_decision = 'PEND_L2') AS pend_l2,
            COUNT(DISTINCT l.payment_id) FILTER (WHERE l.final_decision = 'PEND_L1') AS pend_l1,
            MAX(l.created_at)                                                   AS last_run_at
        FROM ptf_scenario_groups sg
        LEFT JOIN ptf_payment_messages_v2 pm ON pm.scenario_group_id = sg.id
        LEFT JOIN latest l ON l.payment_id = pm.id
        GROUP BY sg.id, sg.group_name, sg.description
        ORDER BY last_run_at DESC NULLS LAST, sg.group_name
    """)
    groups = rows_to_dicts(cur)
    cur.close()
    return {"groups": groups}


@app.get("/api/alerts")
def get_alerts(
    group: Optional[str] = Query(None, description="Filter by group_name"),
    decision: Optional[str] = Query(None, description="Filter by final_decision (PASS_L1, PEND_L2, PEND_L1)"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """
    List screening alerts (latest result per payment) with full context.
    Supports filtering by group and/or decision.
    """
    conn = get_db()
    cur = conn.cursor()

    where_clauses = []
    params = []

    if group:
        where_clauses.append("sg.group_name = %s")
        params.append(group)
    if decision:
        where_clauses.append("sr.final_decision = %s")
        params.append(decision)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    cur.execute(f"""
        WITH latest_results AS (
            SELECT DISTINCT ON (sr.payment_id)
                sr.id          AS result_id,
                sr.payment_id,
                sr.scenario_id,
                sr.final_decision,
                sr.narrative_summary,
                sr.elapsed_time_ms,
                sr.created_at  AS screened_at
            FROM ptf_screening_results_v2 sr
            ORDER BY sr.payment_id, sr.id DESC
        )
        SELECT
            lr.result_id,
            lr.scenario_id,
            lr.final_decision,
            lr.narrative_summary,
            lr.elapsed_time_ms,
            lr.screened_at,
            pm.id               AS payment_id,
            pm.is_true_positive,
            pm.payment_json,
            sg.group_name,
            sg.id               AS group_id,
            CASE
                WHEN (pm.is_true_positive = 1 AND lr.final_decision = 'PEND_L2')
                  OR (pm.is_true_positive = 0 AND lr.final_decision = 'PASS_L1')
                THEN true ELSE false
            END AS is_correct
        FROM latest_results lr
        JOIN ptf_payment_messages_v2 pm ON pm.id = lr.payment_id
        JOIN ptf_scenario_groups sg ON sg.id = pm.scenario_group_id
        {where_sql}
        ORDER BY lr.screened_at DESC
        LIMIT %s OFFSET %s
    """, params + [limit, offset])

    alerts = rows_to_dicts(cur)

    # Total count for pagination
    cur.execute(f"""
        WITH latest_results AS (
            SELECT DISTINCT ON (sr.payment_id)
                sr.payment_id,
                sr.final_decision
            FROM ptf_screening_results_v2 sr
            ORDER BY sr.payment_id, sr.id DESC
        )
        SELECT COUNT(*)
        FROM latest_results lr
        JOIN ptf_payment_messages_v2 pm ON pm.id = lr.payment_id
        JOIN ptf_scenario_groups sg ON sg.id = pm.scenario_group_id
        {where_sql}
    """, params)
    total = cur.fetchone()[0]

    cur.close()
    return {"total": total, "limit": limit, "offset": offset, "alerts": alerts}


@app.get("/api/alerts/{scenario_id}")
def get_alert_detail(scenario_id: str):
    """Full detail for a single alert — all screening runs, investigator + verifier outputs."""
    conn = get_db()
    cur = conn.cursor()

    # Payment metadata
    cur.execute("""
        SELECT pm.id, pm.scenario_id, pm.is_true_positive, pm.payment_json,
               pm.screening_flags, pm.sanctions_hit_data, pm.mock_worldcheck_entities,
               sg.group_name
        FROM ptf_payment_messages_v2 pm
        JOIN ptf_scenario_groups sg ON sg.id = pm.scenario_group_id
        WHERE pm.scenario_id = %s
        LIMIT 1
    """, (scenario_id,))
    pm_row = cur.fetchone()
    if not pm_row:
        raise HTTPException(status_code=404, detail=f"Scenario '{scenario_id}' not found")

    pm_cols = [d[0] for d in cur.description]
    payment = dict(zip(pm_cols, pm_row))
    if isinstance(payment.get("created_at"), datetime):
        payment["created_at"] = payment["created_at"].isoformat()

    # All screening results for this payment (most recent first)
    cur.execute("""
        SELECT sr.id, sr.run_id, sr.final_decision, sr.investigator_output,
               sr.verifier_output, sr.narrative_summary, sr.elapsed_time_ms, sr.created_at
        FROM ptf_screening_results_v2 sr
        WHERE sr.payment_id = %s
        ORDER BY sr.id DESC
    """, (pm_row[0],))
    results = rows_to_dicts(cur)

    cur.close()
    return {"payment": payment, "screening_results": results}


@app.get("/api/accuracy")
def get_accuracy(group: Optional[str] = Query(None)):
    """
    Accuracy scorecard — correct/incorrect breakdown per group (or overall).
    Correct = (is_true_positive=1 → PEND_L2) or (is_true_positive=0 → PASS_L1).
    """
    conn = get_db()
    cur = conn.cursor()

    where_sql = "WHERE sg.group_name = %s" if group else ""
    params = [group] if group else []

    cur.execute(f"""
        WITH latest AS (
            SELECT DISTINCT ON (sr.payment_id)
                sr.payment_id,
                sr.final_decision
            FROM ptf_screening_results_v2 sr
            ORDER BY sr.payment_id, sr.id DESC
        )
        SELECT
            sg.group_name,
            COUNT(*)                                                              AS total,
            COUNT(*) FILTER (
                WHERE (pm.is_true_positive = 1 AND l.final_decision = 'PEND_L2')
                   OR (pm.is_true_positive = 0 AND l.final_decision = 'PASS_L1')
            )                                                                     AS correct,
            COUNT(*) FILTER (WHERE l.final_decision = 'PEND_L2')                AS pend_l2,
            COUNT(*) FILTER (WHERE l.final_decision = 'PASS_L1')                AS pass_l1,
            COUNT(*) FILTER (WHERE l.final_decision = 'PEND_L1')                AS pend_l1,
            COUNT(*) FILTER (WHERE pm.is_true_positive = 1 AND l.final_decision = 'PASS_L1') AS false_negatives,
            COUNT(*) FILTER (WHERE pm.is_true_positive = 0 AND l.final_decision = 'PEND_L2') AS false_positives
        FROM latest l
        JOIN ptf_payment_messages_v2 pm ON pm.id = l.payment_id
        JOIN ptf_scenario_groups sg ON sg.id = pm.scenario_group_id
        {where_sql}
        GROUP BY sg.group_name
        ORDER BY sg.group_name
    """, params)

    rows = rows_to_dicts(cur)
    for r in rows:
        r["accuracy_pct"] = round(r["correct"] / r["total"] * 100, 1) if r["total"] else None

    cur.close()
    return {"groups": rows}


@app.get("/api/kb-audit")
def get_kb_audit(limit: int = Query(50, ge=1, le=200)):
    """Recent Knowledge Base audit entries from the Intelligence Layer."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM ptf_kb_audit_v2
        ORDER BY id DESC
        LIMIT %s
    """, (limit,))
    rows = rows_to_dicts(cur)
    cur.close()
    return {"entries": rows}


@app.get("/api/runs")
def get_runs(
    group: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    """Recent screening runs with result counts."""
    conn = get_db()
    cur = conn.cursor()

    where_sql = ""
    params: list = []
    if group:
        where_sql = "WHERE sg.group_name = %s"
        params.append(group)

    cur.execute(f"""
        SELECT
            r.id,
            r.run_label,
            r.created_at,
            sg.group_name,
            COUNT(sr.id)                                                    AS result_count,
            COUNT(sr.id) FILTER (WHERE sr.final_decision = 'PASS_L1')      AS passed,
            COUNT(sr.id) FILTER (WHERE sr.final_decision = 'PEND_L2')      AS pend_l2
        FROM ptf_screening_runs_v2 r
        LEFT JOIN ptf_screening_results_v2 sr ON sr.run_id = r.id
        LEFT JOIN ptf_payment_messages_v2 pm ON pm.id = sr.payment_id
        LEFT JOIN ptf_scenario_groups sg ON sg.id = pm.scenario_group_id
        {where_sql}
        GROUP BY r.id, r.run_label, r.created_at, sg.group_name
        ORDER BY r.created_at DESC
        LIMIT %s
    """, params + [limit])

    runs = rows_to_dicts(cur)
    cur.close()
    return {"runs": runs}


# ── HITL Review endpoint ─────────────────────────────────────────────────────

from pydantic import BaseModel

class ReviewPayload(BaseModel):
    scenario_id: str
    decision: str          # CLOSE_CLEAR | CLOSE_ESCALATE | CLOSE_BLOCK
    reviewer: str
    notes: str = ""

@app.post("/api/review")
def post_review(payload: ReviewPayload):
    """
    Submit a human review decision for a PEND_L1/PEND_L2 case.
    Writes a review record and marks the case as human-reviewed.
    """
    import json as _json
    allowed = {"CLOSE_CLEAR", "CLOSE_ESCALATE", "CLOSE_BLOCK"}
    if payload.decision not in allowed:
        raise HTTPException(status_code=400, detail=f"decision must be one of {allowed}")

    conn = get_db()
    cur = conn.cursor()

    # Ensure table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ptf_human_reviews (
            id            SERIAL PRIMARY KEY,
            scenario_id   TEXT NOT NULL,
            decision      TEXT NOT NULL,
            reviewer      TEXT NOT NULL,
            notes         TEXT,
            reviewed_at   TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    cur.execute("""
        INSERT INTO ptf_human_reviews (scenario_id, decision, reviewer, notes)
        VALUES (%s, %s, %s, %s)
        RETURNING id, reviewed_at
    """, (payload.scenario_id, payload.decision, payload.reviewer, payload.notes))
    row = cur.fetchone()
    review_id, reviewed_at = row

    cur.close()
    return {
        "ok": True,
        "review_id": review_id,
        "scenario_id": payload.scenario_id,
        "decision": payload.decision,
        "reviewed_at": reviewed_at.isoformat()
    }


def get_audit_db():
    """Returns a connection to the agent-managed DB — single source of truth for ptf_kb_audit_v2."""
    return get_db()


@app.get("/api/insights")
def get_insights(limit: int = Query(20, ge=1, le=100)):
    """
    Intelligence Layer KB proposals from ptf_kb_audit_v2.
    Returns full proposal detail including before/after KB text and compliance rationale.
    """
    conn = get_audit_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, batch_id, status, proposed_changes, evidence,
               approved_by, rejection_reason, resolved_at, created_at,
               conversation_id
        FROM ptf_kb_audit_v2
        ORDER BY id DESC
        LIMIT %s
    """, (limit,))
    rows = rows_to_dicts(cur)
    cur.close()
    conn.close()
    return {"insights": rows}



@app.get("/api/kb")
def get_kb():
    """Return the current knowledge base (kb.md) content for display in the dashboard."""
    kb_path = "/home/banking-demo/skills/sanctions-screening/kb.md"
    try:
        with open(kb_path, "r") as f:
            content = f.read()
        return {"content": content, "path": kb_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read KB: {e}")

class InsightApproval(BaseModel):
    audit_id: int
    action: str    # approve | reject
    reviewer: str

@app.post("/api/insights/review")
def review_insight(payload: InsightApproval):
    """
    Approve or reject an Intelligence Layer KB proposal from ptf_kb_audit_v2.
    Note: approval here marks the record in the audit DB. The Intelligence Layer agent
    then runs apply_changes.py to actually update kb.md on the next HITL callback.
    """
    if payload.action not in {"approve", "reject"}:
        raise HTTPException(status_code=400, detail="action must be 'approve' or 'reject'")

    conn = get_audit_db()
    cur = conn.cursor()

    # Check record exists and is pending
    cur.execute("SELECT id, status FROM ptf_kb_audit_v2 WHERE id = %s", (payload.audit_id,))
    existing = cur.fetchone()
    if not existing:
        raise HTTPException(status_code=404, detail=f"Audit entry {payload.audit_id} not found")

    new_status = "approved" if payload.action == "approve" else "rejected"
    cur.execute("""
        UPDATE ptf_kb_audit_v2
        SET status = %s, approved_by = %s, resolved_at = NOW()
        WHERE id = %s
        RETURNING id, status, approved_by, resolved_at
    """, (new_status, payload.reviewer, payload.audit_id))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    result = {
        "ok": True,
        "audit_id": row[0],
        "status": row[1],
        "approved_by": row[2],
        "resolved_at": row[3].isoformat() if row[3] else None
    }

    # Fire Zamp webhook to apply KB changes immediately (approval path only)
    if payload.action == "approve":
        try:
            import urllib.request as _ur, json as _json
            _body = _json.dumps({"audit_id": row[0], "approved_by": row[2]}).encode()
            _req  = _ur.Request(
                "https://api-us.zamp.ai/triggers/hooks/cTL50-OAr7m_eK6IHABqnDmSJzZiQHE5UHm3JKJHIpA",
                data=_body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with _ur.urlopen(_req, timeout=5) as _resp:
                pass
        except Exception as _e:
            # Non-fatal — approval is already recorded; webhook failure just delays KB apply
            print(f"[webhook] Warning: KB apply webhook call failed: {_e}")

    return result


# ── Resume Intelligence Layer agent via Conversations API ───────────────────

class ResumePayload(BaseModel):
    audit_id: int
    action: str   # approve | reject
    reviewer: str
    reason: Optional[str] = None

@app.post("/api/insights/resume")
def resume_intelligence_agent(payload: ResumePayload):
    """
    Resume a paused Intelligence Layer agent by sending a follow-up message
    to its Conversations API session.

    Looks up the conversation_id stored in ptf_kb_audit_v2, then POSTs
    the reviewer's decision to POST /api/v1/conversations/{id}/messages.

    Returns ok=True on success, with the conversation_id used.
    """
    if payload.action not in {"approve", "reject"}:
        raise HTTPException(status_code=400, detail="action must be 'approve' or 'reject'")

    # Look up the conversation_id for this audit record
    conn = get_audit_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, status, conversation_id FROM ptf_kb_audit_v2 WHERE id = %s",
        (payload.audit_id,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail=f"Audit entry {payload.audit_id} not found")

    audit_id, current_status, conversation_id = row

    if not conversation_id:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Audit entry {audit_id} has no conversation_id. "
                "The Intelligence Layer agent may not have stored it yet, "
                "or this batch pre-dates the conversation tracking feature."
            )
        )

    # Build the message to send to the waiting agent
    if payload.action == "approve":
        message = (
            f"APPROVED by {payload.reviewer}. "
            "Please apply the KB changes now using apply_changes.py."
        )
    else:
        reason_text = payload.reason or "No reason provided"
        message = (
            f"REJECTED by {payload.reviewer}. Reason: {reason_text}. "
            "Please reject the proposals using apply_changes.py --reject."
        )

    # Call the Conversations API to resume the agent
    zamp_base = os.environ.get("ZAMP_BASE_URL", "https://api-us.zamp.ai")
    zamp_token = os.environ.get("ZAMP_API_KEY")

    if not zamp_token:
        raise HTTPException(status_code=503, detail="ZAMP_API_KEY not configured on server")

    import urllib.request as _ur
    import json as _json

    url = f"{zamp_base}/api/v1/conversations/{conversation_id}/messages"
    body = _json.dumps({"message": message}).encode()
    req = _ur.Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {zamp_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with _ur.urlopen(req, timeout=10) as resp:
            resp_body = _json.loads(resp.read().decode())
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to resume agent conversation: {e}"
        )

    return {
        "ok": True,
        "audit_id": audit_id,
        "conversation_id": conversation_id,
        "action": payload.action,
        "message_sent": message,
        "api_response": resp_body,
    }
