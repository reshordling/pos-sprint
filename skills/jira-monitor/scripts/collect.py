#!/usr/bin/env python3
"""
Stage 1: COLLECT — Query DuckDB and produce raw metrics JSON.

Usage:
    python collect.py PILOT --db /path/to/analytics.duckdb --config /path/to/flow_config.json
    python collect.py PILOT  # uses defaults from environment or auto-discovery

Output: JSON to stdout.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    import duckdb
except ImportError:
    print(json.dumps({"error": "duckdb not installed. Run: pip install duckdb"}), file=sys.stdout)
    sys.exit(1)


def find_db_path(project_key: str, db_path: str | None = None) -> Path:
    """Resolve DuckDB file path."""
    if db_path:
        return Path(db_path)
    root = os.environ.get("INSIGHTSFACTORY_ROOT", "")
    if root:
        return Path(root) / "exports" / project_key.upper() / "analytics.duckdb"
    # Auto-discover: look for exports/ in current dir or parent dirs
    for candidate in [Path.cwd(), Path.cwd().parent, Path.cwd().parent.parent]:
        p = candidate / "exports" / project_key.upper() / "analytics.duckdb"
        if p.exists():
            return p
    return Path("exports") / project_key.upper() / "analytics.duckdb"


def find_config_path(config_path: str | None = None) -> Path:
    """Resolve flow_config.json path."""
    if config_path:
        return Path(config_path)
    root = os.environ.get("INSIGHTSFACTORY_ROOT", "")
    if root:
        return Path(root) / "flow_config.json"
    for candidate in [Path.cwd(), Path.cwd().parent, Path.cwd().parent.parent]:
        p = candidate / "flow_config.json"
        if p.exists():
            return p
    return Path("flow_config.json")


def load_config(config_path: Path, project_key: str) -> dict:
    """Load project config from flow_config.json."""
    if not config_path.exists():
        return {"error": f"Config not found: {config_path}. Run setup_config.py first."}
    with open(config_path, "r", encoding="utf-8") as f:
        full_config = json.load(f)
    projects = full_config.get("projects", {})
    cfg = projects.get(project_key.upper(), {})
    if not cfg:
        available = list(projects.keys())
        return {"error": f"Project '{project_key}' not in config. Available: {available}"}
    return cfg


def extract_config_statuses(cfg: dict) -> dict:
    """Extract status lists from config."""
    mapping = cfg.get("status_mapping", {})
    return {
        "backlog": mapping.get("backlog", []),
        "commitment": mapping.get("commitment", []),
        "active": mapping.get("active", []),
        "waiting": mapping.get("waiting", []),
        "done": mapping.get("done", []),
        "wip": cfg.get("wip_statuses", []),
        "stuck_threshold_days": cfg.get("stuck_threshold_days", 14),
        "throughput_period": cfg.get("throughput_period", "week"),
    }


def sql_in(values: list[str]) -> str:
    """Build SQL IN clause from list of strings."""
    if not values:
        return "('')"
    escaped = [v.replace("'", "''") for v in values]
    return "(" + ", ".join(f"'{v}'" for v in escaped) + ")"


def collect_computed_metrics(con, project: str) -> dict:
    """Collect CLT, LT, CT, FE from computed_metrics table."""
    rows = con.execute("""
        SELECT issue_key, customer_lt_days, lead_time_days, cycle_time_days,
               flow_efficiency_pct, done_at, first_commitment_at, first_active_at
        FROM computed_metrics
        WHERE project_key = ? AND done_at IS NOT NULL
        ORDER BY done_at
    """, [project]).fetchall()

    result = {"clt": [], "lt": [], "ct": [], "fe": [], "by_week": {}, "issues": []}
    for row in rows:
        issue_key, clt, lt, ct, fe, done_at, commit_at, active_at = row
        week_key = done_at[:10] if done_at else None
        if week_key:
            try:
                dt = datetime.fromisoformat(done_at.replace("Z", "+00:00").split("+")[0])
                week_key = f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
            except (ValueError, IndexError):
                week_key = None

        entry = {"key": issue_key, "done_at": done_at}
        if clt is not None:
            result["clt"].append(clt)
            entry["clt"] = round(clt, 1)
        if lt is not None:
            result["lt"].append(lt)
            entry["lt"] = round(lt, 1)
        if ct is not None:
            result["ct"].append(ct)
            entry["ct"] = round(ct, 1)
        if fe is not None:
            result["fe"].append(fe)
            entry["fe"] = round(fe, 1)

        if week_key:
            wk = result["by_week"].setdefault(week_key, {"clt": [], "lt": [], "ct": [], "fe": [], "count": 0})
            wk["count"] += 1
            if clt is not None: wk["clt"].append(clt)
            if lt is not None: wk["lt"].append(lt)
            if ct is not None: wk["ct"].append(ct)
            if fe is not None: wk["fe"].append(fe)

        result["issues"].append(entry)

    # Aggregate by_week to averages
    for wk_key, wk_data in result["by_week"].items():
        for metric in ["clt", "lt", "ct", "fe"]:
            vals = wk_data[metric]
            wk_data[metric] = round(sum(vals) / len(vals), 1) if vals else None

    return result


def collect_throughput(con, project: str) -> dict:
    """Throughput by week."""
    rows = con.execute("""
        SELECT DATE_TRUNC('week', CAST(done_at AS TIMESTAMP)) as period,
               COUNT(*) as completed
        FROM computed_metrics
        WHERE project_key = ? AND done_at IS NOT NULL
        GROUP BY period
        ORDER BY period
    """, [project]).fetchall()

    by_week = {}
    for row in rows:
        if row[0]:
            dt = row[0]
            week_key = f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
            by_week[week_key] = row[1]
    return {"by_week": by_week}


def collect_wip(con, project: str, wip_statuses: list[str]) -> dict:
    """Current WIP breakdown."""
    if not wip_statuses:
        return {"total": 0, "by_status": {}, "by_assignee": {}}

    rows = con.execute(f"""
        SELECT status, assignee, issue_key, summary, updated_at
        FROM issues
        WHERE project_key = ? AND status IN {sql_in(wip_statuses)}
    """, [project]).fetchall()

    by_status = {}
    by_assignee = {}
    items = []
    for row in rows:
        status, assignee, key, summary, updated = row
        by_status[status] = by_status.get(status, 0) + 1
        name = assignee or "Unassigned"
        by_assignee[name] = by_assignee.get(name, 0) + 1
        items.append({"key": key, "summary": summary, "status": status, "assignee": name})

    return {
        "total": len(rows),
        "by_status": dict(sorted(by_status.items(), key=lambda x: -x[1])),
        "by_assignee": dict(sorted(by_assignee.items(), key=lambda x: -x[1])),
        "items": items,
    }


def collect_avg_time_in_status(con, project: str) -> dict:
    """Average dwell time per status from transitions."""
    rows = con.execute("""
        WITH status_durations AS (
            SELECT issue_key, status_to as status,
                   transition_at as entered,
                   LEAD(transition_at) OVER (PARTITION BY issue_key ORDER BY transition_at) as exited
            FROM status_transitions
            WHERE project_key = ?
        )
        SELECT status,
               AVG(DATEDIFF('hour', CAST(entered AS TIMESTAMP), CAST(exited AS TIMESTAMP))) as avg_hours,
               PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY
                   DATEDIFF('hour', CAST(entered AS TIMESTAMP), CAST(exited AS TIMESTAMP))
               ) as p85_hours,
               COUNT(*) as sample_size
        FROM status_durations
        WHERE exited IS NOT NULL AND entered IS NOT NULL
        GROUP BY status
        ORDER BY avg_hours DESC
    """, [project]).fetchall()

    result = {}
    for row in rows:
        status, avg_h, p85_h, sample = row
        if status and avg_h is not None:
            result[status] = {
                "avg_hours": round(float(avg_h), 1),
                "p85_hours": round(float(p85_h), 1) if p85_h else None,
                "sample_size": sample,
            }
    return result


def collect_review_queue(con, project: str, review_statuses: list[str]) -> dict:
    """Time spent in review statuses."""
    if not review_statuses:
        return {"avg_hours": 0, "p85_hours": 0, "sample_size": 0}

    rows = con.execute(f"""
        WITH review_enters AS (
            SELECT issue_key, status_to, transition_at as entered_at,
                   LEAD(transition_at) OVER (PARTITION BY issue_key ORDER BY transition_at) as left_at
            FROM status_transitions
            WHERE project_key = ? AND status_to IN {sql_in(review_statuses)}
        )
        SELECT
            AVG(DATEDIFF('hour', CAST(entered_at AS TIMESTAMP),
                COALESCE(CAST(left_at AS TIMESTAMP), CURRENT_TIMESTAMP))) as avg_hours,
            PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY
                DATEDIFF('hour', CAST(entered_at AS TIMESTAMP),
                    COALESCE(CAST(left_at AS TIMESTAMP), CURRENT_TIMESTAMP))
            ) as p85_hours,
            COUNT(*) as sample_size
        FROM review_enters
        WHERE entered_at IS NOT NULL
    """, [project]).fetchall()

    if rows and rows[0][0] is not None:
        return {
            "avg_hours": round(float(rows[0][0]), 1),
            "p85_hours": round(float(rows[0][1]), 1) if rows[0][1] else None,
            "sample_size": rows[0][2],
        }
    return {"avg_hours": 0, "p85_hours": 0, "sample_size": 0}


def collect_defect_containment(con, project: str, done_statuses: list[str]) -> dict:
    """Defect containment ratio."""
    bug_types = ("'Bug'", "'Ошибка'", "'Баг'", "'Дефект'")
    rows = con.execute(f"""
        SELECT
            COUNT(CASE WHEN status NOT IN {sql_in(done_statuses)} THEN 1 END) as pre_release,
            COUNT(*) as total
        FROM issues
        WHERE project_key = ? AND issue_type IN ({','.join(bug_types)})
    """, [project]).fetchall()

    if rows and rows[0][1] > 0:
        return {
            "pct": round(rows[0][0] / rows[0][1] * 100, 1),
            "pre_release": rows[0][0],
            "total": rows[0][1],
        }
    return {"pct": 0, "pre_release": 0, "total": 0}


def collect_stuck_issues(con, project: str, wip_statuses: list[str], threshold_days: int) -> list:
    """Find issues stuck in a status longer than threshold."""
    if not wip_statuses:
        return []

    rows = con.execute(f"""
        WITH latest_transition AS (
            SELECT issue_key, status_to, transition_at,
                   ROW_NUMBER() OVER (PARTITION BY issue_key ORDER BY transition_at DESC) as rn
            FROM status_transitions
            WHERE project_key = ?
        )
        SELECT lt.issue_key, i.summary, lt.status_to, lt.transition_at, i.assignee
        FROM latest_transition lt
        JOIN issues i ON lt.issue_key = i.issue_key AND i.project_key = ?
        WHERE lt.rn = 1
          AND lt.status_to IN {sql_in(wip_statuses)}
          AND DATEDIFF('day', CAST(lt.transition_at AS TIMESTAMP), CURRENT_TIMESTAMP) >= ?
        ORDER BY lt.transition_at ASC
    """, [project, project, threshold_days]).fetchall()

    result = []
    for row in rows:
        key, summary, status, transition_at, assignee = row
        days = 0
        if transition_at:
            try:
                dt = datetime.fromisoformat(transition_at.replace("Z", "+00:00").split("+")[0])
                days = (datetime.utcnow() - dt).days
            except (ValueError, IndexError):
                pass
        result.append({
            "key": key,
            "summary": summary,
            "status": status,
            "days_in_status": days,
            "assignee": assignee or "Unassigned",
        })
    return result


def collect_ct_tech_ready(con, project: str, done_statuses: list[str]) -> dict:
    """Cycle Time Tech Ready — time from any commitment/active to done.
    Uses the longest waiting+active span before done as proxy for Tech Ready."""
    # Get top issues by cycle time from computed_metrics
    rows = con.execute("""
        SELECT cm.issue_key, i.summary, cm.cycle_time_days, cm.done_at
        FROM computed_metrics cm
        JOIN issues i ON cm.issue_key = i.issue_key AND i.project_key = cm.project_key
        WHERE cm.project_key = ? AND cm.done_at IS NOT NULL AND cm.cycle_time_days IS NOT NULL
        ORDER BY cm.cycle_time_days DESC
        LIMIT 10
    """, [project]).fetchall()

    top_slowest = []
    for row in rows:
        top_slowest.append({
            "key": row[0],
            "summary": row[1],
            "days": round(float(row[2]), 1),
            "done_at": row[3],
        })
    return {"top_slowest": top_slowest[:5]}


def collect_issue_count(con, project: str) -> dict:
    """Basic issue counts."""
    row = con.execute("""
        SELECT COUNT(*) as total,
               COUNT(CASE WHEN resolution_date IS NOT NULL THEN 1 END) as resolved
        FROM issues WHERE project_key = ?
    """, [project]).fetchone()
    return {"total": row[0], "resolved": row[1], "open": row[0] - row[1]}


# ============================================================
# Extended Metrics (v2)
# ============================================================

def collect_worklog_metrics(con, project: str) -> dict:
    """Worklog analysis: hours by person, by week, by type, overload detection."""
    # Hours by person per week (last 8 weeks)
    person_weeks = con.execute("""
        SELECT author,
               DATE_TRUNC('week', CAST(started_at AS TIMESTAMP)) as week,
               SUM(time_spent_seconds) / 3600.0 as hours
        FROM worklogs
        WHERE project_key = ?
          AND CAST(started_at AS TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL 56 DAY
        GROUP BY author, week
        ORDER BY week DESC, hours DESC
    """, [project]).fetchall()

    by_person_week = {}
    for author, week, hours in person_weeks:
        if not author:
            continue
        wk = f"{week.isocalendar()[0]}-W{week.isocalendar()[1]:02d}" if week else "unknown"
        by_person_week.setdefault(author, {})[wk] = round(hours, 1)

    # Overload detection: avg weekly hours per person (last 4 weeks)
    overload = con.execute("""
        WITH weekly AS (
            SELECT author,
                   DATE_TRUNC('week', CAST(started_at AS TIMESTAMP)) as week,
                   SUM(time_spent_seconds) / 3600.0 as hours
            FROM worklogs
            WHERE project_key = ?
              AND CAST(started_at AS TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL 28 DAY
            GROUP BY author, week
        )
        SELECT author, AVG(hours) as avg_weekly, MAX(hours) as max_weekly, COUNT(*) as weeks_active
        FROM weekly GROUP BY author ORDER BY avg_weekly DESC
    """, [project]).fetchall()

    people = []
    for author, avg_w, max_w, weeks in overload:
        if not author:
            continue
        people.append({
            "author": author,
            "avg_weekly_hours": round(avg_w, 1),
            "max_weekly_hours": round(max_w, 1),
            "weeks_active": weeks,
            "overloaded": avg_w > 40,
            "underloaded": avg_w < 20 and weeks >= 2,
        })

    # Hours by issue type
    by_type = {}
    for r in con.execute("""
        SELECT i.issue_type,
               SUM(w.time_spent_seconds) / 3600.0 as hours,
               COUNT(DISTINCT w.issue_key) as issues
        FROM worklogs w
        JOIN issues i ON w.project_key = i.project_key AND w.issue_key = i.issue_key
        WHERE w.project_key = ?
        GROUP BY i.issue_type ORDER BY hours DESC
    """, [project]).fetchall():
        by_type[r[0]] = {"hours": round(r[1], 1), "issues": r[2]}

    # Total and per-person totals
    total = con.execute(
        "SELECT COALESCE(SUM(time_spent_seconds),0)/3600.0 FROM worklogs WHERE project_key=?",
        [project],
    ).fetchone()[0]

    return {
        "total_hours": round(total, 1),
        "people": people,
        "by_person_week": by_person_week,
        "by_type": by_type,
    }


def collect_estimate_accuracy(con, project: str) -> dict:
    """Compare actual time spent vs original estimates."""
    rows = con.execute("""
        SELECT issue_key, issue_type, assignee,
               json_extract(payload, '$.fields.timeoriginalestimate')::BIGINT as est_sec,
               json_extract(payload, '$.fields.timespent')::BIGINT as spent_sec,
               summary
        FROM issues
        WHERE project_key = ?
          AND json_extract(payload, '$.fields.timeoriginalestimate')::BIGINT > 0
          AND json_extract(payload, '$.fields.timespent')::BIGINT > 0
          AND resolution_date IS NOT NULL
    """, [project]).fetchall()

    if not rows:
        return {"sample_size": 0}

    ratios = []
    by_person = {}
    by_type = {}
    worst_underestimates = []

    for key, itype, assignee, est, spent, summary in rows:
        ratio = spent / est
        ratios.append(ratio)

        name = assignee or "Unassigned"
        bp = by_person.setdefault(name, {"ratios": [], "count": 0})
        bp["ratios"].append(ratio)
        bp["count"] += 1

        bt = by_type.setdefault(itype or "Unknown", {"ratios": [], "count": 0})
        bt["ratios"].append(ratio)
        bt["count"] += 1

        if ratio > 2.0:
            worst_underestimates.append({
                "key": key, "summary": summary, "ratio": round(ratio, 2),
                "estimated_h": round(est / 3600, 1), "actual_h": round(spent / 3600, 1),
            })

    worst_underestimates.sort(key=lambda x: -x["ratio"])

    def ratio_stats(vals):
        s = sorted(vals)
        n = len(s)
        return {
            "avg_ratio": round(sum(s) / n, 2),
            "median_ratio": round(s[n // 2], 2),
            "under_estimated_pct": round(100.0 * sum(1 for v in s if v > 1.2) / n, 1),
            "over_estimated_pct": round(100.0 * sum(1 for v in s if v < 0.8) / n, 1),
            "sample_size": n,
        }

    person_summary = {}
    for name, data in by_person.items():
        person_summary[name] = ratio_stats(data["ratios"])

    type_summary = {}
    for itype, data in by_type.items():
        type_summary[itype] = ratio_stats(data["ratios"])

    return {
        **ratio_stats(ratios),
        "by_person": person_summary,
        "by_type": type_summary,
        "worst_underestimates": worst_underestimates[:10],
    }


def collect_defect_metrics(con, project: str, done_statuses: list[str], profile: dict | None) -> dict:
    """Defect metrics: containment, velocity, time-to-fix, breakdown by labels."""
    # Determine bug types from profile or use defaults
    bug_types = ["Bug", "Ошибка", "Баг", "Дефект", "Defect"]
    if profile:
        custom = profile.get("type_mapping", {}).get("bug_types")
        if custom:
            bug_types = custom

    bug_type_in = sql_in(bug_types)

    # Total bugs
    total_row = con.execute(f"""
        SELECT COUNT(*),
               COUNT(CASE WHEN resolution_date IS NOT NULL THEN 1 END)
        FROM issues WHERE project_key=? AND issue_type IN {bug_type_in}
    """, [project]).fetchone()
    total_bugs = total_row[0]
    resolved_bugs = total_row[1]

    if total_bugs == 0:
        return {"total_bugs": 0, "note": "No bugs found with types: " + str(bug_types)}

    # Bug velocity by week (last 12 weeks)
    bug_velocity = {}
    for r in con.execute(f"""
        SELECT DATE_TRUNC('week', CAST(created_at AS TIMESTAMP)) as week, COUNT(*)
        FROM issues
        WHERE project_key=? AND issue_type IN {bug_type_in}
          AND CAST(created_at AS TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL 84 DAY
        GROUP BY week ORDER BY week
    """, [project]).fetchall():
        if r[0]:
            wk = f"{r[0].isocalendar()[0]}-W{r[0].isocalendar()[1]:02d}"
            bug_velocity[wk] = r[1]

    # Bug resolution velocity
    bug_resolved_velocity = {}
    for r in con.execute(f"""
        SELECT DATE_TRUNC('week', CAST(resolution_date AS TIMESTAMP)) as week, COUNT(*)
        FROM issues
        WHERE project_key=? AND issue_type IN {bug_type_in} AND resolution_date IS NOT NULL
          AND CAST(resolution_date AS TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL 84 DAY
        GROUP BY week ORDER BY week
    """, [project]).fetchall():
        if r[0]:
            wk = f"{r[0].isocalendar()[0]}-W{r[0].isocalendar()[1]:02d}"
            bug_resolved_velocity[wk] = r[1]

    # Time-to-fix for bugs (from computed_metrics)
    ttf_rows = con.execute(f"""
        SELECT cm.cycle_time_days
        FROM computed_metrics cm
        JOIN issues i ON cm.project_key=i.project_key AND cm.issue_key=i.issue_key
        WHERE cm.project_key=? AND i.issue_type IN {bug_type_in}
          AND cm.cycle_time_days IS NOT NULL
    """, [project]).fetchall()
    ttf_values = [r[0] for r in ttf_rows]

    ttf_stats = {}
    if ttf_values:
        s = sorted(ttf_values)
        n = len(s)
        ttf_stats = {
            "avg_days": round(sum(s) / n, 1),
            "median_days": round(s[n // 2], 1),
            "p85_days": round(s[int(n * 0.85)], 1) if n > 1 else round(s[0], 1),
            "sample_size": n,
        }

    # Defect containment by labels (if profile defines them)
    containment = {}
    pre_release_labels = []
    post_release_labels = []
    if profile:
        pre_release_labels = profile.get("defect_containment", {}).get("pre_release_labels", [])
        post_release_labels = profile.get("defect_containment", {}).get("post_release_labels", [])

    if pre_release_labels or post_release_labels:
        # Count bugs with pre-release labels
        pre_count = 0
        post_count = 0
        if pre_release_labels:
            for label in pre_release_labels:
                r = con.execute(f"""
                    SELECT COUNT(*) FROM issues
                    WHERE project_key=? AND issue_type IN {bug_type_in}
                      AND labels::VARCHAR LIKE ?
                """, [project, f'%"{label}"%']).fetchone()
                pre_count += r[0]
        if post_release_labels:
            for label in post_release_labels:
                r = con.execute(f"""
                    SELECT COUNT(*) FROM issues
                    WHERE project_key=? AND issue_type IN {bug_type_in}
                      AND labels::VARCHAR LIKE ?
                """, [project, f'%"{label}"%']).fetchone()
                post_count += r[0]

        total_labeled = pre_count + post_count
        containment = {
            "pre_release_count": pre_count,
            "post_release_count": post_count,
            "containment_pct": round(100.0 * pre_count / total_labeled, 1) if total_labeled > 0 else None,
            "pre_release_labels": pre_release_labels,
            "post_release_labels": post_release_labels,
        }
    else:
        # Auto-detect: look for common label patterns
        label_counts = {}
        for r in con.execute(f"""
            SELECT j.value::VARCHAR as lbl, COUNT(*)
            FROM issues, json_each(issues.labels::JSON) as j
            WHERE project_key=? AND issue_type IN {bug_type_in}
            GROUP BY lbl ORDER BY COUNT(*) DESC LIMIT 20
        """, [project]).fetchall():
            label_counts[r[0].strip('"')] = r[1]
        containment = {
            "note": "No containment labels configured. Configure in project_profile.json.",
            "bug_labels_found": label_counts,
        }

    # Bugs by priority
    by_priority = {}
    for r in con.execute(f"""
        SELECT priority, COUNT(*) FROM issues
        WHERE project_key=? AND issue_type IN {bug_type_in}
        GROUP BY priority ORDER BY COUNT(*) DESC
    """, [project]).fetchall():
        by_priority[r[0]] = r[1]

    # Bug ratio (bugs as % of all issues)
    total_issues = con.execute(
        "SELECT COUNT(*) FROM issues WHERE project_key=?", [project]
    ).fetchone()[0]
    bug_ratio = round(100.0 * total_bugs / total_issues, 1) if total_issues > 0 else 0

    return {
        "total_bugs": total_bugs,
        "resolved_bugs": resolved_bugs,
        "open_bugs": total_bugs - resolved_bugs,
        "bug_ratio_pct": bug_ratio,
        "by_priority": by_priority,
        "time_to_fix": ttf_stats,
        "created_by_week": bug_velocity,
        "resolved_by_week": bug_resolved_velocity,
        "containment": containment,
    }


def collect_sprint_scope(con, project: str) -> dict:
    """Detect sprint scope changes (tasks added after sprint start)."""
    # Extract sprint changes from payload changelog
    sprint_data = {}  # sprint_name -> {start_date, issues_at_start, issues_added_later}

    rows = con.execute("""
        SELECT issue_key, payload FROM issues
        WHERE project_key=? AND payload::VARCHAR LIKE '%Sprint%'
    """, [project]).fetchall()

    for issue_key, payload_raw in rows:
        try:
            payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
        except (json.JSONDecodeError, TypeError):
            continue

        for history in (payload.get("changelog") or {}).get("histories", []):
            created = history.get("created")
            for item in history.get("items", []):
                if item.get("field", "").lower() != "sprint":
                    continue

                to_val = item.get("toString") or ""
                from_val = item.get("fromString") or ""

                # Find sprints the issue was ADDED to
                added_to = set()
                for s in to_val.split(","):
                    s = s.strip()
                    if s and s not in from_val:
                        added_to.add(s)

                for sprint_name in added_to:
                    sd = sprint_data.setdefault(sprint_name, {
                        "additions": [],
                        "first_seen": None,
                        "total_added": 0,
                    })
                    sd["additions"].append({
                        "issue_key": issue_key,
                        "added_at": created,
                    })
                    sd["total_added"] += 1
                    if not sd["first_seen"] or (created and created < sd["first_seen"]):
                        sd["first_seen"] = created

    if not sprint_data:
        return {"has_sprints": False}

    # For each sprint: find earliest addition (≈sprint start), then count late additions
    sprint_summary = []
    for sprint_name, data in sorted(sprint_data.items()):
        additions = sorted(data["additions"], key=lambda x: x["added_at"] or "")
        if not additions:
            continue

        # Heuristic: first batch of additions (within 24h of first) = planned
        # Everything after = scope change
        first_ts = additions[0]["added_at"]
        if not first_ts:
            continue

        try:
            first_dt = datetime.fromisoformat(first_ts.replace("Z", "+00:00").split("+")[0])
        except (ValueError, IndexError):
            continue

        planned = []
        scope_change = []
        for a in additions:
            if not a["added_at"]:
                continue
            try:
                a_dt = datetime.fromisoformat(a["added_at"].replace("Z", "+00:00").split("+")[0])
            except (ValueError, IndexError):
                continue

            if (a_dt - first_dt).total_seconds() <= 86400:  # within 24h
                planned.append(a["issue_key"])
            else:
                scope_change.append({
                    "issue_key": a["issue_key"],
                    "added_at": a["added_at"],
                    "days_after_start": round((a_dt - first_dt).total_seconds() / 86400, 1),
                })

        sprint_summary.append({
            "sprint": sprint_name,
            "start_approx": first_ts,
            "planned_count": len(planned),
            "scope_change_count": len(scope_change),
            "scope_change_pct": round(100.0 * len(scope_change) / (len(planned) + len(scope_change)), 1)
            if (len(planned) + len(scope_change)) > 0 else 0,
            "scope_changes": scope_change[:10],  # limit details
        })

    # Recent sprints only (last 10)
    sprint_summary.sort(key=lambda x: x["start_approx"], reverse=True)
    recent = sprint_summary[:10]

    # Average scope change across recent sprints
    avg_scope_pct = 0
    if recent:
        avg_scope_pct = round(
            sum(s["scope_change_pct"] for s in recent) / len(recent), 1
        )

    return {
        "has_sprints": True,
        "total_sprints_found": len(sprint_summary),
        "avg_scope_change_pct": avg_scope_pct,
        "recent_sprints": recent,
    }


def collect_type_breakdown(con, project: str) -> dict:
    """Metrics broken down by issue type."""
    rows = con.execute("""
        SELECT i.issue_type,
               COUNT(*) as total,
               COUNT(CASE WHEN i.resolution_date IS NOT NULL THEN 1 END) as resolved,
               AVG(cm.customer_lt_days) as avg_clt,
               AVG(cm.cycle_time_days) as avg_ct,
               AVG(cm.flow_efficiency_pct) as avg_fe
        FROM issues i
        LEFT JOIN computed_metrics cm
            ON i.project_key = cm.project_key AND i.issue_key = cm.issue_key
        WHERE i.project_key = ?
        GROUP BY i.issue_type
        ORDER BY total DESC
    """, [project]).fetchall()

    breakdown = {}
    for itype, total, resolved, avg_clt, avg_ct, avg_fe in rows:
        if not itype:
            continue
        breakdown[itype] = {
            "total": total,
            "resolved": resolved,
            "open": total - resolved,
            "avg_clt_days": round(avg_clt, 1) if avg_clt else None,
            "avg_ct_days": round(avg_ct, 1) if avg_ct else None,
            "avg_fe_pct": round(avg_fe, 1) if avg_fe else None,
        }

    return breakdown


def main():
    parser = argparse.ArgumentParser(description="Collect raw metrics from DuckDB")
    parser.add_argument("project", help="Project key (e.g. PILOT)")
    parser.add_argument("--db", help="Path to analytics.duckdb")
    parser.add_argument("--config", help="Path to flow_config.json")
    parser.add_argument("--profile", help="Path to project_profile.json (for extended metrics)")
    args = parser.parse_args()

    project = args.project.upper()

    # Resolve paths
    db_path = find_db_path(project, args.db)
    config_path = find_config_path(args.config)

    # Load config
    cfg = load_config(config_path, project)
    if "error" in cfg:
        print(json.dumps({"error": cfg["error"]}, ensure_ascii=False))
        sys.exit(1)

    statuses = extract_config_statuses(cfg)

    # Load project profile (optional, for extended metrics)
    profile = None
    profile_candidates = []
    if args.profile:
        profile_candidates.append(Path(args.profile))
    # Auto-discover profile next to DB
    profile_candidates.append(db_path.parent / "project_profile.json")
    for profile_path in profile_candidates:
        if profile_path.exists():
            try:
                profile = json.loads(profile_path.read_text(encoding="utf-8"))
                print(f"Loaded profile: {profile_path}", file=sys.stderr)
                break
            except (json.JSONDecodeError, OSError):
                print(f"Warning: could not load profile {profile_path}", file=sys.stderr)

    # Check DB exists
    if not db_path.exists():
        print(json.dumps({
            "error": f"Database not found: {db_path}. Run InsightsFactory export first."
        }, ensure_ascii=False))
        sys.exit(1)

    # Connect read-only
    con = duckdb.connect(str(db_path), read_only=True)

    try:
        # Derive review statuses from waiting statuses (those containing review-related words)
        review_keywords = {"ревью", "review", "код-ревью", "koд-ревью", "code review"}
        review_statuses = [s for s in statuses["waiting"]
                          if any(kw in s.lower() for kw in review_keywords)]
        # If no review statuses found, use all waiting statuses
        if not review_statuses:
            review_statuses = statuses["waiting"]

        # Collect core metrics
        computed = collect_computed_metrics(con, project)
        throughput = collect_throughput(con, project)
        wip = collect_wip(con, project, statuses["wip"])
        avg_time = collect_avg_time_in_status(con, project)
        review_queue = collect_review_queue(con, project, review_statuses)
        defect = collect_defect_containment(con, project, statuses["done"])
        stuck = collect_stuck_issues(con, project, statuses["wip"], statuses["stuck_threshold_days"])
        ct_tech = collect_ct_tech_ready(con, project, statuses["done"])
        counts = collect_issue_count(con, project)

        # Collect extended metrics (v2)
        worklog_metrics = collect_worklog_metrics(con, project)
        estimate_accuracy = collect_estimate_accuracy(con, project)
        defect_metrics = collect_defect_metrics(con, project, statuses["done"], profile)
        sprint_scope = collect_sprint_scope(con, project)
        type_breakdown = collect_type_breakdown(con, project)

    finally:
        con.close()

    # Compute summary stats from raw values
    def safe_stats(values):
        if not values:
            return {"avg": None, "median": None, "p85": None, "sample_size": 0}
        s = sorted(values)
        n = len(s)
        return {
            "avg": round(sum(s) / n, 1),
            "median": round(s[n // 2], 1),
            "p85": round(s[int(n * 0.85)], 1) if n > 1 else round(s[0], 1),
            "sample_size": n,
        }

    output = {
        "project": project,
        "collected_at": datetime.utcnow().isoformat() + "Z",
        "db_path": str(db_path),
        "profile": profile or {},
        "config": {
            "active_statuses": statuses["active"],
            "waiting_statuses": statuses["waiting"],
            "done_statuses": statuses["done"],
            "commitment_statuses": statuses["commitment"],
            "wip_statuses": statuses["wip"],
            "review_statuses": review_statuses,
            "stuck_threshold_days": statuses["stuck_threshold_days"],
        },
        "issue_counts": counts,
        "metrics": {
            "clt": safe_stats(computed["clt"]),
            "lt": safe_stats(computed["lt"]),
            "ct": safe_stats(computed["ct"]),
            "fe": safe_stats(computed["fe"]),
            "throughput": throughput,
            "wip": wip,
            "avg_time_in_status": avg_time,
            "review_queue": review_queue,
            "defect_containment": defect,
            "ct_top_slowest": ct_tech["top_slowest"],
            # Extended metrics (v2)
            "worklogs": worklog_metrics,
            "estimate_accuracy": estimate_accuracy,
            "defects": defect_metrics,
            "sprint_scope": sprint_scope,
            "type_breakdown": type_breakdown,
        },
        "by_week": computed["by_week"],
        "stuck_issues": stuck,
    }

    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
