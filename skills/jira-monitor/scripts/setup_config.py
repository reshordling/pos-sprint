#!/usr/bin/env python3
"""
Setup flow_config.json for a new project.

Usage:
    python setup_config.py PILOT --db /path/to/analytics.duckdb --config /path/to/flow_config.json

Discovers all unique statuses from DuckDB and asks the user to categorize them.
Can also be used non-interactively by providing a JSON mapping via --mapping.
"""

import argparse
import json
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("Error: duckdb not installed. Run: pip install duckdb", file=sys.stderr)
    sys.exit(1)


def discover_statuses(db_path: str, project: str) -> list[str]:
    """Get all unique statuses from issues and transitions."""
    con = duckdb.connect(db_path, read_only=True)
    statuses = set()
    try:
        # Current statuses
        rows = con.execute(
            "SELECT DISTINCT status FROM issues WHERE project_key = ? AND status IS NOT NULL",
            [project]
        ).fetchall()
        for r in rows:
            statuses.add(r[0])

        # Historical statuses from transitions
        rows = con.execute("""
            SELECT DISTINCT s FROM (
                SELECT status_from as s FROM status_transitions WHERE project_key = ? AND status_from IS NOT NULL
                UNION
                SELECT status_to as s FROM status_transitions WHERE project_key = ? AND status_to IS NOT NULL
            )
        """, [project, project]).fetchall()
        for r in rows:
            statuses.add(r[0])
    finally:
        con.close()

    return sorted(statuses)


def interactive_setup(statuses: list[str]) -> dict:
    """Interactively categorize statuses."""
    print("\nFound statuses:")
    for i, s in enumerate(statuses, 1):
        print(f"  {i}. {s}")

    print("\nCategorize each status. Enter comma-separated numbers for each category.")
    print("Press Enter to skip a category.\n")

    categories = {}
    for cat, desc in [
        ("backlog", "Backlog (not yet committed)"),
        ("commitment", "Commitment (ready to start, queued)"),
        ("active", "Active work (development, testing)"),
        ("waiting", "Waiting (review, blocked, queued between stages)"),
        ("done", "Done (released, closed)"),
    ]:
        nums = input(f"  {desc}: ").strip()
        if nums:
            indices = [int(n.strip()) - 1 for n in nums.split(",") if n.strip().isdigit()]
            categories[cat] = [statuses[i] for i in indices if 0 <= i < len(statuses)]
        else:
            categories[cat] = []

    # WIP statuses = commitment + active + waiting
    wip = categories["commitment"] + categories["active"] + categories["waiting"]
    print(f"\nWIP statuses (auto-derived): {wip}")

    threshold = input("Stuck threshold in days [14]: ").strip()
    threshold = int(threshold) if threshold.isdigit() else 14

    period = input("Throughput period (week/sprint) [week]: ").strip() or "week"

    return {
        "status_mapping": categories,
        "wip_statuses": wip,
        "throughput_period": period,
        "stuck_threshold_days": threshold,
    }


def main():
    parser = argparse.ArgumentParser(description="Setup flow_config.json for a project")
    parser.add_argument("project", help="Project key (e.g. PILOT)")
    parser.add_argument("--db", required=True, help="Path to analytics.duckdb")
    parser.add_argument("--config", default="flow_config.json", help="Path to flow_config.json")
    parser.add_argument("--mapping", help="JSON file with pre-built mapping (non-interactive)")
    parser.add_argument("--discover-only", action="store_true",
                        help="Only discover statuses and output JSON (no config modification)")
    args = parser.parse_args()

    project = args.project.upper()
    config_path = Path(args.config)

    # Discover statuses
    statuses = discover_statuses(args.db, project)
    if not statuses:
        print(f"No statuses found for project {project} in {args.db}", file=sys.stderr)
        sys.exit(1)

    # Discover-only mode: output JSON and exit
    if args.discover_only:
        print(json.dumps({"project": project, "statuses": statuses}, ensure_ascii=False, indent=2))
        return

    # Get mapping
    if args.mapping:
        with open(args.mapping, "r", encoding="utf-8") as f:
            project_cfg = json.load(f)
    else:
        project_cfg = interactive_setup(statuses)

    # Load or create config file
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            full_config = json.load(f)
    else:
        full_config = {"version": 1, "projects": {}}

    # Update project
    full_config.setdefault("projects", {})[project] = {
        **project_cfg,
        "business_params": None,
        "status_source_mode": "auto",
        "status_source_project": project,
        "retention_snapshots": 14,
        "auto_export": {
            "enabled": False,
            "mode": "delta",
            "time": "03:00",
            "days": ["mon", "tue", "wed", "thu", "fri"],
            "max_retries": 3,
        },
    }

    # Save
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(full_config, f, ensure_ascii=False, indent=2)

    print(f"\nConfig saved to {config_path}")
    print(f"Project {project} configured with {len(statuses)} statuses.")


if __name__ == "__main__":
    main()
