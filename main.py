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
               conversation_id, trigger_source
        FROM ptf_kb_audit_v2
        ORDER BY id DESC
        LIMIT %s
    """, (limit,))
    rows = rows_to_dicts(cur)

    # Tab counts
    cur2 = conn.cursor()
    cur2.execute("""
        SELECT trigger_source, COUNT(*) FROM ptf_kb_audit_v2
        WHERE status = 'pending'
        GROUP BY trigger_source
    """)
    tab_counts = {r[0]: r[1] for r in cur2.fetchall()}
    cur2.close()
    cur.close()
    conn.close()
    return {
        "insights": rows,
        "tab_counts": {
            "pend_l1": tab_counts.get("pend_l1", 0),
            "pend_l2_false_positive": tab_counts.get("pend_l2_false_positive", 0),
            "pattern": tab_counts.get("pattern", 0),
        }
    }



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

    # Notify Intelligence Layer via Conversations API so it runs apply_changes.py
    if payload.action in {"approve", "reject"}:
        try:
            import urllib.request as _ur, json as _json
            _ZAMP_TOKEN = os.environ.get("ZAMP_API_KEY", "zamp_sk_a26cd5e1-bb86-4bc4-80fc-053b26f92a3f_rZYAQ2gbmHpMIMdZI7JLae8_es_ODNL6P72zoPCCQEWCjg7NyyiN8LsoBRlEPS3z")
            _ZAMP_BASE  = os.environ.get("ZAMP_BASE_URL", "https://api-us.zamp.ai")
            _CONV_ID    = "70992790-b11e-4d6e-a85c-a85b4693d34e"
            if payload.action == "approve":
                _msg = (
                    f"APPROVED by {row[2]}. audit_id: {row[0]}. "
                    f"Please apply the KB changes now using apply_changes.py "
                    f"--audit-id {row[0]} --approved-by \"{row[2]}\"."
                )
            else:
                _msg = (
                    f"REJECTED by {row[2]}. audit_id: {row[0]}. "
                    f"Reason: No reason provided. "
                    f"Please reject the proposals using apply_changes.py "
                    f"--audit-id {row[0]} --reject --reason \"No reason provided\"."
                )
            _url  = f"{_ZAMP_BASE}/api/v1/conversations/{_CONV_ID}/messages"
            _body = _json.dumps({"message": _msg}).encode()
            _req  = _ur.Request(_url, data=_body,
                headers={"Authorization": f"Bearer {_ZAMP_TOKEN}", "Content-Type": "application/json"},
                method="POST")
            with _ur.urlopen(_req, timeout=5) as _resp:
                pass
        except Exception as _e:
            # Non-fatal — DB record already updated; agent notification is best-effort
            print(f"[review] Warning: Conversations API notify failed: {_e}")

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

    # Always use the standing conversation — one fixed channel for all batches.
    # The audit_id in the message tells the agent which batch to act on.
    STANDING_CONVERSATION_ID = "70992790-b11e-4d6e-a85c-a85b4693d34e"
    conversation_id = STANDING_CONVERSATION_ID

    # Build the message — audit_id is included so the agent knows which record to apply
    if payload.action == "approve":
        message = (
            f"APPROVED by {payload.reviewer}. audit_id: {audit_id}. "
            "Please apply the KB changes now using apply_changes.py "
            f"--audit-id {audit_id} --approved-by \"{payload.reviewer}\"."
        )
    else:
        reason_text = payload.reason or "No reason provided"
        message = (
            f"REJECTED by {payload.reviewer}. audit_id: {audit_id}. "
            f"Reason: {reason_text}. "
            "Please reject the proposals using apply_changes.py "
            f"--audit-id {audit_id} --reject --reason \"{reason_text}\"."
        )

    # Call the Conversations API to resume the agent
    zamp_base = os.environ.get("ZAMP_BASE_URL", "https://api-us.zamp.ai")
    zamp_token = os.environ.get("ZAMP_API_KEY", "zamp_sk_a26cd5e1-bb86-4bc4-80fc-053b26f92a3f_rZYAQ2gbmHpMIMdZI7JLae8_es_ODNL6P72zoPCCQEWCjg7NyyiN8LsoBRlEPS3z")

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
    notify_ok = False
    resp_body = None
    notify_error = None
    try:
        with _ur.urlopen(req, timeout=10) as resp:
            resp_body = _json.loads(resp.read().decode())
            notify_ok = True
    except Exception as e:
        notify_error = str(e)
        print(f"[resume] Warning: Conversations API notify failed (best-effort): {e}")

    # Update audit record status regardless of notify result
    try:
        _conn = get_audit_db()
        _cur = _conn.cursor()
        _new_status = "approved" if payload.action == "approve" else "rejected"
        _cur.execute(
            "UPDATE ptf_kb_audit_v2 SET status = %s, approved_by = %s, resolved_at = NOW() WHERE id = %s",
            (_new_status, payload.reviewer, audit_id)
        )
        _conn.commit()
        _cur.close()
        _conn.close()
    except Exception as db_e:
        print(f"[resume] Warning: DB status update failed: {db_e}")

    return {
        "ok": True,
        "audit_id": audit_id,
        "conversation_id": conversation_id,
        "action": payload.action,
        "message_sent": message,
        "notify_sent": notify_ok,
        "notify_error": notify_error,
        "api_response": resp_body,
    }


# ── PEND_L2 False Positive Close endpoint ─────────────────────────────────

class PendL2ClosePayload(BaseModel):
    scenario_id: str
    reviewer: str
    notes: str = ""

@app.post("/api/pend-l2/close-false-positive")
def close_pend_l2_false_positive(payload: PendL2ClosePayload):
    """
    Called when a PEND_L2 case is resolved as a false positive by an analyst.
    1. Records the disposition in ptf_human_reviews.
    2. Creates a pending ptf_kb_audit_v2 record (trigger_source=pend_l2_false_positive).
    3. Runs analyse_pend_l2_fp.py to enrich the record with full case analysis.
    4. Sends the structured report to the Intelligence Layer standing conversation.
    """
    import json as _json
    import subprocess as _sp

    conn = get_db()
    cur = conn.cursor()

    # 1. Record the false-positive disposition
    cur.execute("""
        INSERT INTO ptf_human_reviews (scenario_id, decision, reviewer, notes)
        VALUES (%s, 'CLOSE_FALSE_POSITIVE', %s, %s)
        RETURNING id, reviewed_at
    """, (payload.scenario_id, payload.reviewer, payload.notes))
    review_id, reviewed_at = cur.fetchone()

    # 2. Fetch case context for evidence summary
    cur.execute("""
        SELECT pm.id, pm.scenario_id, pm.is_true_positive, pm.payment_json,
               sg.group_name, sr.final_decision, sr.narrative_summary
        FROM ptf_payment_messages_v2 pm
        JOIN ptf_scenario_groups sg ON sg.id = pm.scenario_group_id
        LEFT JOIN LATERAL (
            SELECT final_decision, narrative_summary
            FROM ptf_screening_results_v2
            WHERE payment_id = pm.id ORDER BY id DESC LIMIT 1
        ) sr ON true
        WHERE pm.scenario_id = %s LIMIT 1
    """, (payload.scenario_id,))
    alert = cur.fetchone()

    if alert:
        _, scenario_id, _, _, group_name, final_decision, narrative = alert
        evidence = (
            f"PEND_L2 false positive: {scenario_id} in group '{group_name}'. "
            f"Engine decision: {final_decision}. Closed as false positive by {payload.reviewer}. "
            f"Notes: {payload.notes or '(none)'}."
        )
        batch_id = f"pend_l2_fp_{scenario_id}"
    else:
        evidence = f"PEND_L2 false positive: {payload.scenario_id}. Closed by {payload.reviewer}."
        batch_id = f"pend_l2_fp_{payload.scenario_id}"

    # 3. Read current KB snapshot
    kb_snapshot = ""
    try:
        with open("/home/banking-demo/skills/sanctions-screening/kb.md") as f:
            kb_snapshot = f.read()
    except Exception:
        pass

    # 4. Create pending audit record
    STANDING_CONVERSATION_ID = "70992790-b11e-4d6e-a85c-a85b4693d34e"
    cur.execute("""
        INSERT INTO ptf_kb_audit_v2
            (batch_id, proposed_changes, evidence, status, before_snapshot, conversation_id, trigger_source)
        VALUES (%s, %s, %s, 'pending', %s, %s, 'pend_l2_false_positive')
        RETURNING id
    """, (
        batch_id,
        _json.dumps({"trigger": "pend_l2_false_positive", "scenario_id": payload.scenario_id, "proposals": []}),
        evidence, kb_snapshot, STANDING_CONVERSATION_ID,
    ))
    audit_id = cur.fetchone()[0]
    conn.commit()
    cur.close()

    # 5. Run analyse_pend_l2_fp.py to enrich the audit record
    script_output = ""
    try:
        result = _sp.run(
            ["python3",
             "/home/banking-demo/skills/ptf-intelligence-layer/scripts/analyse_pend_l2_fp.py",
             "--scenario-id", payload.scenario_id,
             "--audit-id", str(audit_id),
             "--reviewer", payload.reviewer],
            capture_output=True, text=True, timeout=30, env={**os.environ},
        )
        script_output = result.stdout
        if result.returncode != 0:
            print(f"[pend_l2_close] analyse stderr: {result.stderr[:500]}")
    except Exception as e:
        print(f"[pend_l2_close] Warning: script failed: {e}")

    # 6. Notify Intelligence Layer with full report
    zamp_base = os.environ.get("ZAMP_BASE_URL", "https://api-us.zamp.ai")
    zamp_token = os.environ.get("ZAMP_API_KEY", "zamp_sk_a26cd5e1-bb86-4bc4-80fc-053b26f92a3f_rZYAQ2gbmHpMIMdZI7JLae8_es_ODNL6P72zoPCCQEWCjg7NyyiN8LsoBRlEPS3z")
    notify_sent = False
    if zamp_token:
        try:
            import urllib.request as _ur
            message = (
                f"PEND_L2 FALSE POSITIVE -- INTELLIGENCE LAYER ANALYSIS REQUEST\n"
                f"audit_id: {audit_id} | scenario_id: {payload.scenario_id} | closed_by: {payload.reviewer}\n"
                f"trigger_source: pend_l2_false_positive\n\n"
                f"Review the escalation cause, identify the KB gap, and propose a targeted KB tightening rule. "
                f"Present via HITL then run apply_changes.py --audit-id {audit_id}.\n\n"
                + (script_output[:6000] if script_output else evidence)
            )
            url = f"{zamp_base}/api/v1/conversations/{STANDING_CONVERSATION_ID}/messages"
            body = _json.dumps({"message": message}).encode()
            req = _ur.Request(url, data=body,
                headers={"Authorization": f"Bearer {zamp_token}", "Content-Type": "application/json"},
                method="POST")
            with _ur.urlopen(req, timeout=10):
                notify_sent = True
        except Exception as e:
            print(f"[pend_l2_close] Warning: notify failed: {e}")

    return {
        "ok": True,
        "review_id": review_id,
        "audit_id": audit_id,
        "trigger_source": "pend_l2_false_positive",
        "scenario_id": payload.scenario_id,
        "reviewed_at": reviewed_at.isoformat(),
        "intelligence_layer_notified": notify_sent,
        "analysis_generated": bool(script_output),
    }


# ── PEND_L1 Conflict trigger endpoint ──────────────────────────────────────

class PendL1TriggerPayload(BaseModel):
    scenario_id: str

@app.post("/api/pend-l1/trigger-analysis")
def trigger_pend_l1_analysis(payload: PendL1TriggerPayload):
    """
    Triggered when a screening result is PEND_L1 (Investigator/Verifier conflict).
    1. Creates a pending ptf_kb_audit_v2 record with trigger_source=pend_l1.
    2. Runs analyse_pend_l1.py to produce the full conflict analysis report.
    3. Sends the report to the Intelligence Layer for KB clarification proposal.
    """
    import json as _json
    import subprocess as _sp

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT pm.id, pm.scenario_id, pm.is_true_positive,
               sg.group_name, sr.final_decision
        FROM ptf_payment_messages_v2 pm
        JOIN ptf_scenario_groups sg ON sg.id = pm.scenario_group_id
        LEFT JOIN LATERAL (
            SELECT final_decision
            FROM ptf_screening_results_v2
            WHERE payment_id = pm.id ORDER BY id DESC LIMIT 1
        ) sr ON true
        WHERE pm.scenario_id = %s LIMIT 1
    """, (payload.scenario_id,))
    alert = cur.fetchone()

    if alert:
        _, scenario_id, _, group_name, final_decision = alert
        evidence = (
            f"PEND_L1 conflict: {scenario_id} in group '{group_name}'. "
            f"Final decision: {final_decision}. Investigator/Verifier disagreement."
        )
        batch_id = f"pend_l1_{scenario_id}"
    else:
        evidence = f"PEND_L1 conflict: {payload.scenario_id}. Case not found in DB."
        batch_id = f"pend_l1_{payload.scenario_id}"

    kb_snapshot = ""
    try:
        with open("/home/banking-demo/skills/sanctions-screening/kb.md") as f:
            kb_snapshot = f.read()
    except Exception:
        pass

    STANDING_CONVERSATION_ID = "70992790-b11e-4d6e-a85c-a85b4693d34e"
    cur.execute("""
        INSERT INTO ptf_kb_audit_v2
            (batch_id, proposed_changes, evidence, status, before_snapshot, conversation_id, trigger_source)
        VALUES (%s, %s, %s, 'pending', %s, %s, 'pend_l1')
        RETURNING id
    """, (
        batch_id,
        _json.dumps({"trigger": "pend_l1", "scenario_id": payload.scenario_id, "proposals": []}),
        evidence, kb_snapshot, STANDING_CONVERSATION_ID,
    ))
    audit_id = cur.fetchone()[0]
    conn.commit()
    cur.close()

    script_output = ""
    try:
        result = _sp.run(
            ["python3",
             "/home/banking-demo/skills/ptf-intelligence-layer/scripts/analyse_pend_l1.py",
             "--scenario-id", payload.scenario_id,
             "--audit-id", str(audit_id)],
            capture_output=True, text=True, timeout=30, env={**os.environ},
        )
        script_output = result.stdout
        if result.returncode != 0:
            print(f"[pend_l1_trigger] analyse stderr: {result.stderr[:500]}")
    except Exception as e:
        print(f"[pend_l1_trigger] Warning: script failed: {e}")

    zamp_base = os.environ.get("ZAMP_BASE_URL", "https://api-us.zamp.ai")
    zamp_token = os.environ.get("ZAMP_API_KEY", "zamp_sk_a26cd5e1-bb86-4bc4-80fc-053b26f92a3f_rZYAQ2gbmHpMIMdZI7JLae8_es_ODNL6P72zoPCCQEWCjg7NyyiN8LsoBRlEPS3z")
    notify_sent = False
    if zamp_token:
        try:
            import urllib.request as _ur
            message = (
                f"PEND_L1 CONFLICT -- INTELLIGENCE LAYER ANALYSIS REQUEST\n"
                f"audit_id: {audit_id} | scenario_id: {payload.scenario_id}\n"
                f"trigger_source: pend_l1\n\n"
                f"Investigator and Verifier disagreed on this case. Identify the KB ambiguity "
                f"and propose a clarification. Present via HITL then run apply_changes.py --audit-id {audit_id}.\n\n"
                + (script_output[:6000] if script_output else evidence)
            )
            url = f"{zamp_base}/api/v1/conversations/{STANDING_CONVERSATION_ID}/messages"
            body = _json.dumps({"message": message}).encode()
            req = _ur.Request(url, data=body,
                headers={"Authorization": f"Bearer {zamp_token}", "Content-Type": "application/json"},
                method="POST")
            with _ur.urlopen(req, timeout=10):
                notify_sent = True
        except Exception as e:
            print(f"[pend_l1_trigger] Warning: notify failed: {e}")

    return {
        "ok": True,
        "audit_id": audit_id,
        "trigger_source": "pend_l1",
        "scenario_id": payload.scenario_id,
        "intelligence_layer_notified": notify_sent,
        "analysis_generated": bool(script_output),
    }
