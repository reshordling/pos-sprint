#!/usr/bin/env python3
"""
Project Discovery — scan DuckDB and build a project profile for analysis.

Usage:
    python discover.py PILOT --db /path/to/analytics.duckdb
    python discover.py PILOT --db /path/to/analytics.duckdb --profile /path/to/project_profile.json

If --profile points to an existing file, outputs a DELTA (what changed since last discovery).
Otherwise outputs a full profile for first-time setup.

The AI agent reads this output and asks the user clarifying questions,
then saves the answers into project_profile.json for the Monitor agent.
"""

import argparse
import json
import os
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("Error: duckdb not installed. Run: pip install duckdb", file=sys.stderr)
    sys.exit(1)


def discover_project(con, project: str) -> dict:
    """Scan DuckDB and return a comprehensive project profile."""

    profile = {"project": project, "discovery": {}}

    # --- Issue Types ---
    types = {}
    for r in con.execute(
        "SELECT issue_type, COUNT(*) FROM issues WHERE project_key=? GROUP BY issue_type ORDER BY COUNT(*) DESC",
        [project],
    ).fetchall():
        types[r[0]] = r[1]
    profile["discovery"]["issue_types"] = types

    # --- Statuses ---
    statuses = {}
    for r in con.execute(
        "SELECT status, COUNT(*) FROM issues WHERE project_key=? GROUP BY status ORDER BY COUNT(*) DESC",
        [project],
    ).fetchall():
        statuses[r[0]] = r[1]
    profile["discovery"]["statuses"] = statuses

    # --- Priorities ---
    priorities = {}
    for r in con.execute(
        "SELECT priority, COUNT(*) FROM issues WHERE project_key=? GROUP BY priority ORDER BY COUNT(*) DESC",
        [project],
    ).fetchall():
        priorities[r[0]] = r[1]
    profile["discovery"]["priorities"] = priorities

    # --- Labels ---
    labels = {}
    try:
        for r in con.execute(
            """SELECT j.value::VARCHAR as lbl, COUNT(*) as cnt
               FROM issues, json_each(issues.labels::JSON) as j
               WHERE project_key=? GROUP BY lbl ORDER BY cnt DESC LIMIT 30""",
            [project],
        ).fetchall():
            labels[r[0].strip('"')] = r[1]
    except Exception:
        pass
    profile["discovery"]["labels"] = labels

    # --- Components ---
    components = {}
    try:
        for r in con.execute(
            """SELECT json_extract_string(j.value, '$.name') as comp, COUNT(*) as cnt
               FROM issues, json_each(issues.components::JSON) as j
               WHERE project_key=? GROUP BY comp ORDER BY cnt DESC LIMIT 20""",
            [project],
        ).fetchall():
            if r[0]:
                components[r[0]] = r[1]
    except Exception:
        pass
    profile["discovery"]["components"] = components

    # --- Worklogs Summary ---
    wl = con.execute(
        """SELECT COUNT(*), COUNT(DISTINCT author), COUNT(DISTINCT issue_key),
                  COALESCE(SUM(time_spent_seconds), 0) / 3600.0
           FROM worklogs WHERE project_key=?""",
        [project],
    ).fetchone()
    profile["discovery"]["worklogs"] = {
        "total_entries": wl[0],
        "unique_authors": wl[1],
        "unique_issues": wl[2],
        "total_hours": round(wl[3], 1),
    }

    # Top authors by hours
    authors = {}
    for r in con.execute(
        """SELECT author, SUM(time_spent_seconds)/3600.0 as hrs
           FROM worklogs WHERE project_key=? GROUP BY author ORDER BY hrs DESC LIMIT 15""",
        [project],
    ).fetchall():
        authors[r[0]] = round(r[1], 1)
    profile["discovery"]["worklog_authors"] = authors

    # --- Estimate Fields ---
    est = con.execute(
        """SELECT
            COUNT(CASE WHEN json_extract(payload, '$.fields.timeoriginalestimate') IS NOT NULL
                       AND json_extract(payload, '$.fields.timeoriginalestimate')::BIGINT > 0 THEN 1 END),
            COUNT(CASE WHEN json_extract(payload, '$.fields.timespent') IS NOT NULL
                       AND json_extract(payload, '$.fields.timespent')::BIGINT > 0 THEN 1 END),
            COUNT(*)
           FROM issues WHERE project_key=?""",
        [project],
    ).fetchone()
    profile["discovery"]["estimates"] = {
        "issues_with_original_estimate": est[0],
        "issues_with_time_spent": est[1],
        "total_issues": est[2],
        "estimate_coverage_pct": round(100.0 * est[0] / est[2], 1) if est[2] > 0 else 0,
    }

    # --- Sprint Data ---
    sprint_count = con.execute(
        """SELECT COUNT(*) FROM issues
           WHERE project_key=? AND payload::VARCHAR LIKE '%Sprint%'""",
        [project],
    ).fetchone()[0]

    sprint_names = set()
    if sprint_count > 0:
        # Extract sprint names from changelog
        rows = con.execute(
            """SELECT payload FROM issues
               WHERE project_key=? AND payload::VARCHAR LIKE '%Sprint%' LIMIT 200""",
            [project],
        ).fetchall()
        for row in rows:
            try:
                payload = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                for h in (payload.get("changelog") or {}).get("histories", []):
                    for item in h.get("items", []):
                        if item.get("field", "").lower() == "sprint":
                            for val in [item.get("toString"), item.get("fromString")]:
                                if val and val.strip():
                                    for s in val.split(","):
                                        s = s.strip()
                                        if s:
                                            sprint_names.add(s)
            except (json.JSONDecodeError, TypeError):
                continue

    profile["discovery"]["sprints"] = {
        "issues_with_sprint_data": sprint_count,
        "sprint_names": sorted(sprint_names)[:30],
        "has_sprints": sprint_count > 0,
    }

    # --- Bug/Defect Analysis ---
    # Find which issue type is "bug" (could be Bug, Ошибка, Баг, Defect, etc.)
    bug_candidates = {}
    bug_keywords = ["bug", "ошибка", "баг", "дефект", "defect", "incident"]
    for itype, count in types.items():
        if itype and any(kw in itype.lower() for kw in bug_keywords):
            bug_candidates[itype] = count

    # Labels that suggest defect lifecycle
    defect_labels = {}
    defect_label_keywords = [
        "prod", "stage", "test", "hotfix", "support", "qa", "regression", "bug",
    ]
    for label, count in labels.items():
        if any(kw in label.lower() for kw in defect_label_keywords):
            defect_labels[label] = count

    profile["discovery"]["defect_signals"] = {
        "bug_type_candidates": bug_candidates,
        "defect_related_labels": defect_labels,
        "total_potential_bugs": sum(bug_candidates.values()),
    }

    # --- Worklog by Issue Type ---
    wl_by_type = {}
    try:
        for r in con.execute(
            """SELECT i.issue_type, SUM(w.time_spent_seconds)/3600.0 as hrs, COUNT(DISTINCT w.issue_key)
               FROM worklogs w JOIN issues i ON w.project_key=i.project_key AND w.issue_key=i.issue_key
               WHERE w.project_key=? GROUP BY i.issue_type ORDER BY hrs DESC""",
            [project],
        ).fetchall():
            wl_by_type[r[0]] = {"hours": round(r[1], 1), "issues": r[2]}
    except Exception:
        pass
    profile["discovery"]["worklog_by_type"] = wl_by_type

    # --- Date Range ---
    dates = con.execute(
        """SELECT MIN(created_at), MAX(created_at), MIN(resolution_date), MAX(resolution_date)
           FROM issues WHERE project_key=?""",
        [project],
    ).fetchone()
    profile["discovery"]["date_range"] = {
        "first_created": dates[0],
        "last_created": dates[1],
        "first_resolved": dates[2],
        "last_resolved": dates[3],
    }

    return profile


def compute_delta(old_profile: dict, new_profile: dict) -> dict:
    """Compare two profiles and return what changed."""
    delta = {"project": new_profile["project"], "is_delta": True, "changes": []}

    old_disc = old_profile.get("discovery", {})
    new_disc = new_profile.get("discovery", {})

    # New issue types
    old_types = set(old_disc.get("issue_types", {}).keys())
    new_types = set(new_disc.get("issue_types", {}).keys())
    if new_types - old_types:
        delta["changes"].append({
            "what": "new_issue_types",
            "items": list(new_types - old_types),
        })

    # New statuses
    old_statuses = set(old_disc.get("statuses", {}).keys())
    new_statuses = set(new_disc.get("statuses", {}).keys())
    if new_statuses - old_statuses:
        delta["changes"].append({
            "what": "new_statuses",
            "items": list(new_statuses - old_statuses),
        })

    # New labels
    old_labels = set(old_disc.get("labels", {}).keys())
    new_labels = set(new_disc.get("labels", {}).keys())
    if new_labels - old_labels:
        delta["changes"].append({
            "what": "new_labels",
            "items": list(new_labels - old_labels),
        })

    # New team members (worklog authors)
    old_authors = set(old_disc.get("worklog_authors", {}).keys())
    new_authors = set(new_disc.get("worklog_authors", {}).keys())
    if new_authors - old_authors:
        delta["changes"].append({
            "what": "new_team_members",
            "items": list(new_authors - old_authors),
        })

    # Significant count changes (>20% shift in type distribution)
    old_total = sum(old_disc.get("issue_types", {}).values()) or 1
    new_total = sum(new_disc.get("issue_types", {}).values()) or 1
    for itype in new_disc.get("issue_types", {}):
        if itype in old_disc.get("issue_types", {}):
            old_pct = old_disc["issue_types"][itype] / old_total * 100
            new_pct = new_disc["issue_types"][itype] / new_total * 100
            if abs(new_pct - old_pct) > 5:
                delta["changes"].append({
                    "what": "type_distribution_shift",
                    "type": itype,
                    "old_pct": round(old_pct, 1),
                    "new_pct": round(new_pct, 1),
                })

    delta["full_profile"] = new_profile
    return delta


def main():
    parser = argparse.ArgumentParser(description="Discover project profile from DuckDB")
    parser.add_argument("project", help="Project key (e.g. PILOT)")
    parser.add_argument("--db", required=True, help="Path to analytics.duckdb")
    parser.add_argument(
        "--profile",
        help="Path to existing project_profile.json (for delta mode)",
    )
    args = parser.parse_args()

    project = args.project.upper()
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect(str(db_path), read_only=True)
    new_profile = discover_project(con, project)
    con.close()

    # Delta mode
    if args.profile:
        profile_path = Path(args.profile)
        if profile_path.exists():
            try:
                old_profile = json.loads(profile_path.read_text(encoding="utf-8"))
                result = compute_delta(old_profile, new_profile)
                print(json.dumps(result, ensure_ascii=False, indent=2))
                return
            except (json.JSONDecodeError, KeyError):
                pass

    # Full discovery mode
    print(json.dumps(new_profile, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
