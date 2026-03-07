#!/usr/bin/env python3
"""
Jira → DuckDB sync with delta updates.

Usage:
    python sync.py PILOT --url https://jira.company.com --token YOUR_PAT
    python sync.py PILOT --full   # force full re-sync
    python sync.py PILOT          # delta sync (only updated since last sync)

Env vars (or .env file in script directory or current directory):
    JIRA_URL        - Jira base URL
    JIRA_EMAIL      - Email (Jira Cloud only)
    JIRA_API_TOKEN  - API token (Cloud) or PAT (Server/DC)

Output: DuckDB file at --db path (default: ./exports/{PROJECT}/analytics.duckdb)
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("Error: requests not installed. Run: pip install requests", file=sys.stderr)
    sys.exit(1)

try:
    import duckdb
except ImportError:
    print("Error: duckdb not installed. Run: pip install duckdb", file=sys.stderr)
    sys.exit(1)

# Load .env from script dir or cwd
for env_path in [Path(__file__).parent.parent / ".env", Path(__file__).parent / ".env", Path.cwd() / ".env"]:
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())
        break


# ============================================================
# Jira API Client
# ============================================================

class JiraClient:
    """Universal Jira REST API client (Cloud + Server/DC)."""

    def __init__(self, base_url: str, email: str | None, token: str, verify_ssl: bool = True):
        self.base_url = base_url.rstrip("/")
        self.is_cloud = "atlassian.net" in base_url.lower()
        self.session = requests.Session()
        self.session.verify = verify_ssl

        if self.is_cloud and email:
            self.session.auth = (email, token)
        elif self.is_cloud:
            self.session.headers["Authorization"] = f"Bearer {token}"
        else:
            # Server/DC: try PAT (Bearer) first
            self.session.headers["Authorization"] = f"Bearer {token}"

        self.session.headers["Accept"] = "application/json"
        self.session.headers["Content-Type"] = "application/json"

    def test_connection(self) -> str | None:
        """Returns display name or None on failure."""
        try:
            r = self.session.get(f"{self.base_url}/rest/api/2/myself", timeout=15)
            if r.status_code == 401 and not self.is_cloud:
                # Fallback: maybe PAT didn't work, but that's all we can try
                return None
            r.raise_for_status()
            data = r.json()
            return data.get("displayName") or data.get("name") or data.get("emailAddress")
        except Exception as e:
            print(f"Connection failed: {e}", file=sys.stderr)
            return None

    def search_issues(self, jql: str, start_at: int = 0, max_results: int = 25,
                      expand: str = "changelog") -> dict:
        """Execute JQL search with pagination."""
        payload = {
            "jql": jql,
            "startAt": start_at,
            "maxResults": max_results,
            "fields": ["*all"],
            "expand": [expand] if expand else [],
        }
        r = self.session.post(
            f"{self.base_url}/rest/api/2/search",
            json=payload,
            timeout=60,
        )
        r.raise_for_status()
        return r.json()

    def fetch_all(self, jql: str, batch_size: int = 25, throttle_sec: float = 1.0) -> list[dict]:
        """Fetch all issues matching JQL with throttling."""
        # First request to get total
        result = self.search_issues(jql, start_at=0, max_results=batch_size)
        total = result.get("total", 0)
        issues = result.get("issues", [])

        if total == 0:
            return []

        print(f"  Total: {total}, batch: {batch_size}, requests: {(total + batch_size - 1) // batch_size}",
              file=sys.stderr)

        start_at = batch_size
        while start_at < total:
            time.sleep(throttle_sec)
            progress = min(start_at + batch_size, total)
            print(f"\r  Loading: {progress}/{total} ({100 * progress // total}%)",
                  end="", file=sys.stderr, flush=True)

            try:
                result = self.search_issues(jql, start_at=start_at, max_results=batch_size)
                batch = result.get("issues", [])
                issues.extend(batch)
            except Exception as e:
                print(f"\n  Error at offset {start_at}: {e}", file=sys.stderr)
                break

            start_at += batch_size

        print(f"\r  Loading: {len(issues)}/{total} (100%)", file=sys.stderr)
        return issues


# ============================================================
# DuckDB Storage (self-contained, no InsightsFactory dependency)
# ============================================================

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS issues (
    project_key VARCHAR NOT NULL,
    issue_key VARCHAR NOT NULL,
    summary VARCHAR,
    status VARCHAR,
    assignee VARCHAR,
    issue_type VARCHAR,
    priority VARCHAR,
    components JSON,
    labels JSON,
    created_at VARCHAR,
    resolution_date VARCHAR,
    updated_at VARCHAR,
    payload JSON,
    PRIMARY KEY (project_key, issue_key)
);
CREATE TABLE IF NOT EXISTS status_transitions (
    project_key VARCHAR NOT NULL,
    issue_key VARCHAR NOT NULL,
    status_from VARCHAR,
    status_to VARCHAR,
    transition_at VARCHAR,
    author VARCHAR
);
CREATE TABLE IF NOT EXISTS computed_metrics (
    project_key VARCHAR NOT NULL,
    issue_key VARCHAR NOT NULL,
    customer_lt_days DOUBLE,
    lead_time_days DOUBLE,
    cycle_time_days DOUBLE,
    flow_efficiency_pct DOUBLE,
    first_commitment_at VARCHAR,
    first_active_at VARCHAR,
    done_at VARCHAR,
    is_reopened BOOLEAN,
    computed_at TIMESTAMP,
    PRIMARY KEY (project_key, issue_key)
);
CREATE TABLE IF NOT EXISTS worklogs (
    project_key VARCHAR NOT NULL,
    issue_key VARCHAR NOT NULL,
    author VARCHAR,
    started_at VARCHAR,
    time_spent_seconds BIGINT
);
CREATE TABLE IF NOT EXISTS sync_metadata (
    project_key VARCHAR NOT NULL PRIMARY KEY,
    last_sync_at VARCHAR,
    last_full_sync_at VARCHAR,
    total_synced INTEGER
);
"""


def ensure_schema(con):
    for stmt in SCHEMA_SQL.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


# ============================================================
# Metric Calculator (minimal, self-contained)
# ============================================================

def parse_date(date_str: str | None) -> datetime | None:
    if not date_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(date_str, fmt).replace(tzinfo=None)
        except ValueError:
            continue
    return None


def get_transitions(issue: dict) -> list[dict]:
    result = []
    for history in (issue.get("changelog") or {}).get("histories", []):
        created = history.get("created")
        for item in history.get("items", []):
            if item.get("field") == "status":
                result.append({
                    "date": parse_date(created),
                    "from": item.get("fromString"),
                    "to": item.get("toString"),
                })
    result.sort(key=lambda x: x["date"] if x["date"] else datetime.min)
    return result


def compute_metrics(issue: dict, done: set, commitment: set, active: set, waiting: set) -> dict:
    """Compute CLT, LT, CT, FE for a single issue. Returns dict with metric values."""
    work_statuses = commitment | active | waiting
    transitions = get_transitions(issue)
    fields = issue.get("fields") or {}
    created = parse_date(fields.get("created"))
    resolution = parse_date(fields.get("resolutiondate"))
    current_status = (fields.get("status") or {}).get("name")

    # Find final done_at
    done_at = None
    done_phase = False
    reopen_count = 0
    for t in transitions:
        if t["date"] and t["to"] in done and not done_phase:
            done_at = t["date"]
            done_phase = True
        elif t["date"] and done_phase and t["to"] not in done and t["to"] in work_statuses:
            reopen_count += 1
            done_phase = False
            done_at = None

    is_done = current_status in done
    if not is_done:
        return {}

    if not done_at:
        done_at = resolution
    if done_at and resolution and resolution < done_at:
        done_at = resolution
    if not done_at:
        return {}

    # CLT: created → done
    clt = None
    if created and done_at:
        clt = max(0.0, (done_at - created).total_seconds() / 86400.0)

    # LT: first commitment → done
    first_commitment = None
    first_active = None
    for t in transitions:
        if not t["date"]:
            continue
        if first_commitment is None and t["to"] in commitment:
            first_commitment = t["date"]
        if first_active is None and t["to"] in active:
            first_active = t["date"]

    # Fallback: issue created in commitment status
    if not first_commitment and transitions:
        initial = transitions[0].get("from")
        if initial and initial in commitment:
            first_commitment = created

    lt = None
    if first_commitment and done_at:
        lt = max(0.0, (done_at - first_commitment).total_seconds() / 86400.0)

    # CT + FE: first active → done, active time only
    if not first_active and transitions:
        initial = transitions[0].get("from")
        if initial and initial in active:
            first_active = created

    ct = None
    fe = None
    if first_active and done_at and done_at >= first_active:
        ct_seconds = (done_at - first_active).total_seconds()
        ct = max(0.0, ct_seconds / 86400.0)

        # FE: sum of time in active statuses / total CT
        active_time = timedelta(0)
        curr_s = transitions[0]["from"] if transitions else current_status
        last_t = first_active
        for t in transitions:
            if not t["date"] or t["date"] <= first_active:
                curr_s = t["to"]
                continue
            seg_end = min(t["date"], done_at)
            if curr_s in active and seg_end > last_t:
                active_time += seg_end - last_t
            if t["date"] >= done_at:
                break
            curr_s = t["to"]
            last_t = t["date"]
        if last_t < done_at and curr_s in active:
            active_time += done_at - last_t

        active_days = max(0.0, active_time.total_seconds() / 86400.0)
        fe = min(100.0, (active_days / ct * 100.0)) if ct > 0 else None

    # Enforce hierarchy: CLT >= LT >= CT
    if ct is not None and lt is not None and lt < ct:
        lt = ct
    if lt is not None and clt is not None and clt < lt:
        clt = lt
    elif ct is not None and clt is not None and clt < ct:
        clt = ct

    return {
        "clt": round(clt, 4) if clt is not None else None,
        "lt": round(lt, 4) if lt is not None else None,
        "ct": round(ct, 4) if ct is not None else None,
        "fe": round(fe, 2) if fe is not None else None,
        "first_commitment_at": first_commitment.isoformat() if first_commitment else None,
        "first_active_at": first_active.isoformat() if first_active else None,
        "done_at": done_at.isoformat() if done_at else None,
        "is_reopened": reopen_count > 0,
    }


# ============================================================
# Upsert Logic
# ============================================================

def upsert_issues(con, project: str, issues: list[dict], config: dict):
    """Upsert issues, transitions, worklogs, and computed metrics into DuckDB."""
    done = set((config.get("status_mapping") or {}).get("done", []))
    commitment = set((config.get("status_mapping") or {}).get("commitment", []))
    active = set((config.get("status_mapping") or {}).get("active", []))
    waiting = set((config.get("status_mapping") or {}).get("waiting", []))

    for issue in issues:
        key = issue.get("key")
        if not key:
            continue
        fields = issue.get("fields") or {}

        # 1. Upsert issue row
        con.execute("""
            INSERT INTO issues (project_key, issue_key, summary, status, assignee,
                issue_type, priority, components, labels, created_at, resolution_date,
                updated_at, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_key, issue_key) DO UPDATE SET
                summary=excluded.summary, status=excluded.status, assignee=excluded.assignee,
                issue_type=excluded.issue_type, priority=excluded.priority,
                components=excluded.components, labels=excluded.labels,
                created_at=excluded.created_at, resolution_date=excluded.resolution_date,
                updated_at=excluded.updated_at, payload=excluded.payload
        """, [
            project, key,
            fields.get("summary"),
            (fields.get("status") or {}).get("name"),
            (fields.get("assignee") or {}).get("displayName"),
            (fields.get("issuetype") or {}).get("name"),
            (fields.get("priority") or {}).get("name"),
            json.dumps(fields.get("components") or [], ensure_ascii=False),
            json.dumps(fields.get("labels") or [], ensure_ascii=False),
            fields.get("created"),
            fields.get("resolutiondate"),
            fields.get("updated"),
            json.dumps(issue, ensure_ascii=False, default=str),
        ])

        # 2. Replace transitions
        con.execute("DELETE FROM status_transitions WHERE project_key=? AND issue_key=?", [project, key])
        for history in (issue.get("changelog") or {}).get("histories", []):
            author_obj = history.get("author") or {}
            author = author_obj.get("displayName") or author_obj.get("name")
            for item in history.get("items", []):
                if item.get("field") == "status":
                    con.execute("""
                        INSERT INTO status_transitions (project_key, issue_key, status_from, status_to, transition_at, author)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, [project, key, item.get("fromString"), item.get("toString"),
                          history.get("created"), author])

        # 3. Replace worklogs
        con.execute("DELETE FROM worklogs WHERE project_key=? AND issue_key=?", [project, key])
        for entry in (fields.get("worklog") or {}).get("worklogs", []):
            wl_author = (entry.get("author") or {}).get("displayName")
            con.execute("""
                INSERT INTO worklogs (project_key, issue_key, author, started_at, time_spent_seconds)
                VALUES (?, ?, ?, ?, ?)
            """, [project, key, wl_author, entry.get("started"), int(entry.get("timeSpentSeconds") or 0)])

        # 4. Compute and upsert metrics
        m = compute_metrics(issue, done, commitment, active, waiting)
        if m:
            con.execute("""
                INSERT INTO computed_metrics (project_key, issue_key, customer_lt_days, lead_time_days,
                    cycle_time_days, flow_efficiency_pct, first_commitment_at, first_active_at,
                    done_at, is_reopened, computed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(project_key, issue_key) DO UPDATE SET
                    customer_lt_days=excluded.customer_lt_days, lead_time_days=excluded.lead_time_days,
                    cycle_time_days=excluded.cycle_time_days, flow_efficiency_pct=excluded.flow_efficiency_pct,
                    first_commitment_at=excluded.first_commitment_at, first_active_at=excluded.first_active_at,
                    done_at=excluded.done_at, is_reopened=excluded.is_reopened, computed_at=excluded.computed_at
            """, [project, key, m["clt"], m["lt"], m["ct"], m["fe"],
                  m["first_commitment_at"], m["first_active_at"], m["done_at"],
                  m["is_reopened"], datetime.now(timezone.utc)])
        else:
            # Issue not done — remove stale metrics if any
            con.execute("DELETE FROM computed_metrics WHERE project_key=? AND issue_key=?", [project, key])


def update_sync_metadata(con, project: str, count: int, is_full: bool):
    now = datetime.now(timezone.utc).isoformat()
    existing = con.execute(
        "SELECT 1 FROM sync_metadata WHERE project_key=?", [project]
    ).fetchone()
    if existing:
        if is_full:
            con.execute("""
                UPDATE sync_metadata SET last_sync_at=?, last_full_sync_at=?, total_synced=?
                WHERE project_key=?
            """, [now, now, count, project])
        else:
            con.execute("""
                UPDATE sync_metadata SET last_sync_at=?, total_synced=total_synced+?
                WHERE project_key=?
            """, [now, count, project])
    else:
        con.execute("""
            INSERT INTO sync_metadata (project_key, last_sync_at, last_full_sync_at, total_synced)
            VALUES (?, ?, ?, ?)
        """, [project, now, now if is_full else None, count])


def get_last_sync(con, project: str) -> str | None:
    row = con.execute(
        "SELECT last_sync_at FROM sync_metadata WHERE project_key=?", [project]
    ).fetchone()
    return row[0] if row else None


# ============================================================
# Config
# ============================================================

def load_config(config_path: str | None, project: str) -> dict:
    """Load project config from flow_config.json."""
    paths_to_try = []
    if config_path:
        paths_to_try.append(Path(config_path))
    paths_to_try.extend([
        Path.cwd() / "flow_config.json",
        Path(__file__).parent.parent / "flow_config.json",
        Path(__file__).parent / "flow_config.json",
    ])

    for p in paths_to_try:
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                full = json.load(f)
            cfg = full.get("projects", {}).get(project.upper(), {})
            if cfg:
                return cfg

    return {}


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Sync Jira project to DuckDB")
    parser.add_argument("project", help="Project key (e.g. PILOT)")
    parser.add_argument("--url", default=os.environ.get("JIRA_URL"), help="Jira base URL")
    parser.add_argument("--email", default=os.environ.get("JIRA_EMAIL"), help="Email (Cloud only)")
    parser.add_argument("--token", default=os.environ.get("JIRA_API_TOKEN"), help="API token or PAT")
    parser.add_argument("--db", help="Path to analytics.duckdb")
    parser.add_argument("--config", help="Path to flow_config.json")
    parser.add_argument("--batch-size", type=int, default=25, help="Issues per request (default: 25)")
    parser.add_argument("--throttle", type=float, default=1.0, help="Seconds between requests (default: 1.0)")
    parser.add_argument("--full", action="store_true", help="Force full re-sync")
    parser.add_argument("--no-verify-ssl", action="store_true", help="Disable SSL verification")
    args = parser.parse_args()

    project = args.project.upper()

    # Validate
    if not args.url:
        print("Error: JIRA_URL required (--url or env var)", file=sys.stderr)
        sys.exit(1)
    if not args.token:
        print("Error: JIRA_API_TOKEN required (--token or env var)", file=sys.stderr)
        sys.exit(1)

    # DB path
    if args.db:
        db_path = Path(args.db)
    else:
        db_path = Path.cwd() / "exports" / project / "analytics.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Load config
    config = load_config(args.config, project)
    if not config.get("status_mapping"):
        print(f"Warning: No config for {project}. Metrics won't be computed.", file=sys.stderr)
        print(f"Run: python scripts/setup_config.py {project} --db {db_path}", file=sys.stderr)

    # Connect to Jira
    print(f"Connecting to {args.url}...", file=sys.stderr)
    client = JiraClient(args.url, args.email, args.token, verify_ssl=not args.no_verify_ssl)
    user = client.test_connection()
    if not user:
        print("Failed to connect to Jira. Check URL and credentials.", file=sys.stderr)
        sys.exit(1)
    print(f"Connected as: {user}", file=sys.stderr)

    # Connect to DuckDB
    con = duckdb.connect(str(db_path))
    ensure_schema(con)

    # Determine JQL
    if args.full:
        jql = f"project = {project} ORDER BY key ASC"
        print(f"Full sync: {project}", file=sys.stderr)
    else:
        last_sync = get_last_sync(con, project)
        if last_sync:
            # Format for JQL: "2026-03-06 13:00"
            try:
                dt = datetime.fromisoformat(last_sync.replace("Z", "+00:00"))
                # Go back 1 hour for safety margin
                dt = dt - timedelta(hours=1)
                jql_date = dt.strftime("%Y-%m-%d %H:%M")
                jql = f'project = {project} AND updated >= "{jql_date}" ORDER BY key ASC'
                print(f"Delta sync since {jql_date}", file=sys.stderr)
            except (ValueError, TypeError):
                jql = f"project = {project} ORDER BY key ASC"
                print(f"Invalid last_sync, falling back to full sync", file=sys.stderr)
                args.full = True
        else:
            jql = f"project = {project} ORDER BY key ASC"
            print(f"First sync (full): {project}", file=sys.stderr)
            args.full = True

    # Fetch
    print(f"JQL: {jql}", file=sys.stderr)
    issues = client.fetch_all(jql, batch_size=args.batch_size, throttle_sec=args.throttle)

    if not issues:
        print("No issues to sync.", file=sys.stderr)
        update_sync_metadata(con, project, 0, args.full)
        con.close()
        return

    # Upsert
    print(f"Upserting {len(issues)} issues into {db_path}...", file=sys.stderr)
    upsert_issues(con, project, issues, config)
    update_sync_metadata(con, project, len(issues), args.full)
    con.close()

    print(f"Done. {len(issues)} issues synced to {db_path}", file=sys.stderr)

    # Output summary as JSON to stdout
    summary = {
        "project": project,
        "synced_at": datetime.now(timezone.utc).isoformat(),
        "issues_synced": len(issues),
        "mode": "full" if args.full else "delta",
        "db_path": str(db_path),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
