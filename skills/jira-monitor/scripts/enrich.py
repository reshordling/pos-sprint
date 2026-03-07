#!/usr/bin/env python3
"""
Stage 3: ENRICH — Add deep context for flagged issues (comments, links, status history).

Usage:
    python collect.py PILOT | python analyze.py | python enrich.py
    python enrich.py < analyzed.json

Reads analyzed JSON from stdin, queries DuckDB for additional context on flagged issues,
outputs enriched JSON.
"""

import json
import sys
from datetime import datetime

try:
    import duckdb
except ImportError:
    print(json.dumps({"error": "duckdb not installed. Run: pip install duckdb"}), file=sys.stdout)
    sys.exit(1)


def get_flagged_issue_keys(data: dict) -> list[str]:
    """Collect all issue keys that need enrichment."""
    keys = set()

    # Stuck issues
    for item in data.get("stuck_issues", []):
        if item.get("key"):
            keys.add(item["key"])

    # Top slowest CT issues
    for item in data.get("metrics", {}).get("ct_top_slowest", []):
        if item.get("key"):
            keys.add(item["key"])

    # Bottleneck stuck issues
    bn = data.get("analysis", {}).get("bottleneck", {})
    for key in bn.get("evidence", {}).get("stuck_issues", []):
        keys.add(key)

    # WIP items in bottleneck status
    bn_stage = bn.get("stage")
    if bn_stage:
        for item in data.get("metrics", {}).get("wip", {}).get("items", []):
            if item.get("status") == bn_stage and item.get("key"):
                keys.add(item["key"])

    return sorted(keys)


def sql_in(values: list[str]) -> str:
    if not values:
        return "('')"
    escaped = [v.replace("'", "''") for v in values]
    return "(" + ", ".join(f"'{v}'" for v in escaped) + ")"


def enrich_comments(con, project: str, issue_keys: list[str]) -> dict:
    """Extract recent comments for flagged issues."""
    result = {}
    if not issue_keys:
        return result

    rows = con.execute(f"""
        SELECT issue_key, json_extract(payload, '$.fields.comment.comments') as comments
        FROM issues
        WHERE project_key = ? AND issue_key IN {sql_in(issue_keys)}
    """, [project]).fetchall()

    for row in rows:
        key = row[0]
        raw_comments = row[1]
        if not raw_comments:
            result[key] = []
            continue

        comments = json.loads(raw_comments) if isinstance(raw_comments, str) else raw_comments
        if not isinstance(comments, list):
            result[key] = []
            continue

        # Take last 3 comments, truncate body
        recent = []
        for c in comments[-3:]:
            author = (c.get("author") or {}).get("displayName", "Unknown")
            body = c.get("body", "")
            if len(body) > 250:
                body = body[:250] + "..."
            created = c.get("created", "")[:19] if c.get("created") else ""
            recent.append({"author": author, "date": created, "text": body})

        result[key] = recent

    return result


def enrich_links(con, project: str, issue_keys: list[str]) -> dict:
    """Extract issue links (blocks, blocked by, relates) for flagged issues."""
    result = {}
    if not issue_keys:
        return result

    rows = con.execute(f"""
        SELECT issue_key, json_extract(payload, '$.fields.issuelinks') as links
        FROM issues
        WHERE project_key = ? AND issue_key IN {sql_in(issue_keys)}
    """, [project]).fetchall()

    for row in rows:
        key = row[0]
        raw_links = row[1]
        if not raw_links:
            result[key] = {"blocks": [], "blocked_by": [], "relates": [], "other": []}
            continue

        links = json.loads(raw_links) if isinstance(raw_links, str) else raw_links
        if not isinstance(links, list):
            result[key] = {"blocks": [], "blocked_by": [], "relates": [], "other": []}
            continue

        parsed = {"blocks": [], "blocked_by": [], "relates": [], "other": []}
        for link in links:
            link_type = (link.get("type") or {}).get("name", "")
            inward_name = (link.get("type") or {}).get("inward", "")
            outward_name = (link.get("type") or {}).get("outward", "")

            # Determine direction and linked issue
            if "outwardIssue" in link:
                linked = link["outwardIssue"]
                direction = "outward"
                relation = outward_name
            elif "inwardIssue" in link:
                linked = link["inwardIssue"]
                direction = "inward"
                relation = inward_name
            else:
                continue

            linked_key = linked.get("key", "")
            linked_summary = (linked.get("fields") or {}).get("summary", "")
            linked_status = ((linked.get("fields") or {}).get("status") or {}).get("name", "")

            entry = {
                "key": linked_key,
                "summary": linked_summary[:100],
                "status": linked_status,
                "relation": relation,
            }

            # Categorize
            if link_type.lower() in ("blocks", "блокирует"):
                if direction == "outward":
                    parsed["blocks"].append(entry)
                else:
                    parsed["blocked_by"].append(entry)
            elif link_type.lower() in ("relates", "связано"):
                parsed["relates"].append(entry)
            else:
                parsed["other"].append(entry)

        result[key] = parsed

    return result


def enrich_status_history(con, project: str, issue_keys: list[str]) -> dict:
    """Get full status transition history for flagged issues."""
    result = {}
    if not issue_keys:
        return result

    rows = con.execute(f"""
        SELECT issue_key, status_from, status_to, transition_at, author
        FROM status_transitions
        WHERE project_key = ? AND issue_key IN {sql_in(issue_keys)}
        ORDER BY issue_key, transition_at
    """, [project]).fetchall()

    # Group by issue
    for row in rows:
        key, sfrom, sto, at, author = row
        if key not in result:
            result[key] = []
        result[key].append({
            "from": sfrom,
            "to": sto,
            "at": at[:19] if at else "",
            "author": author,
        })

    # Calculate time in each status
    enriched = {}
    for key, transitions in result.items():
        status_times = {}
        for i, t in enumerate(transitions):
            entered = t["at"]
            exited = transitions[i + 1]["at"] if i + 1 < len(transitions) else None
            status = t["to"]
            if entered and exited:
                try:
                    dt_in = datetime.fromisoformat(entered)
                    dt_out = datetime.fromisoformat(exited)
                    hours = (dt_out - dt_in).total_seconds() / 3600
                    status_times[status] = status_times.get(status, 0) + hours
                except (ValueError, TypeError):
                    pass
            elif entered and not exited:
                # Still in this status
                try:
                    dt_in = datetime.fromisoformat(entered)
                    hours = (datetime.utcnow() - dt_in).total_seconds() / 3600
                    status_times[status] = status_times.get(status, 0) + hours
                except (ValueError, TypeError):
                    pass

        enriched[key] = {
            "transitions": transitions,
            "time_in_status": {k: round(v, 1) for k, v in sorted(
                status_times.items(), key=lambda x: -x[1]
            )},
        }

    return enriched


def enrich_issue_details(con, project: str, issue_keys: list[str]) -> dict:
    """Get story points, assignee, reporter, type for flagged issues."""
    result = {}
    if not issue_keys:
        return result

    rows = con.execute(f"""
        SELECT issue_key, summary, status, assignee, issue_type, priority,
               json_extract_string(payload, '$.fields.reporter.displayName') as reporter,
               json_extract(payload, '$.fields.customfield_10107') as story_points
        FROM issues
        WHERE project_key = ? AND issue_key IN {sql_in(issue_keys)}
    """, [project]).fetchall()

    for row in rows:
        key = row[0]
        sp = row[7]
        try:
            sp_val = float(sp) if sp and sp != "null" else None
        except (ValueError, TypeError):
            sp_val = None

        result[key] = {
            "summary": row[1],
            "status": row[2],
            "assignee": row[3] or "Unassigned",
            "issue_type": row[4],
            "priority": row[5],
            "reporter": row[6],
            "story_points": sp_val,
        }

    return result


def main():
    raw = sys.stdin.read()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON input: {e}"}), file=sys.stdout)
        sys.exit(1)

    if "error" in data:
        print(json.dumps(data, ensure_ascii=False))
        sys.exit(1)

    project = data.get("project", "")
    db_path = data.get("db_path", "")

    if not db_path:
        print(json.dumps({"error": "No db_path in input data"}), file=sys.stdout)
        sys.exit(1)

    # Get flagged issues
    flagged_keys = get_flagged_issue_keys(data)

    if not flagged_keys:
        data["enrichment"] = {}
        print(json.dumps(data, ensure_ascii=False, indent=2, default=str))
        return

    # Connect to DB
    con = duckdb.connect(db_path, read_only=True)

    try:
        comments = enrich_comments(con, project, flagged_keys)
        links = enrich_links(con, project, flagged_keys)
        history = enrich_status_history(con, project, flagged_keys)
        details = enrich_issue_details(con, project, flagged_keys)
    finally:
        con.close()

    # Merge into enrichment section
    enrichment = {}
    for key in flagged_keys:
        enrichment[key] = {
            **(details.get(key, {})),
            "comments_recent": comments.get(key, []),
            "links": links.get(key, {"blocks": [], "blocked_by": [], "relates": [], "other": []}),
            "status_history": history.get(key, {"transitions": [], "time_in_status": {}}),
        }

    data["enrichment"] = enrichment

    print(json.dumps(data, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
