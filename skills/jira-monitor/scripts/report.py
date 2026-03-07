#!/usr/bin/env python3
"""
Stage 5: REPORT — Generate Markdown digest from enriched JSON (headless mode).

Usage:
    python collect.py PILOT | python analyze.py | python enrich.py | python report.py
    python report.py --save /path/to/reports/  < enriched.json

Reads enriched JSON from stdin, outputs Markdown report.
With --save, also writes the report to a file.
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


def format_digest(data: dict) -> str:
    """Generate Markdown digest from enriched data."""
    project = data.get("project", "???")
    date = datetime.utcnow().strftime("%Y-%m-%d")
    analysis = data.get("analysis", {})
    metrics = data.get("metrics", {})
    alerts = analysis.get("alerts", [])
    bottleneck = analysis.get("bottleneck")
    summary = analysis.get("summary", {})
    enrichment = data.get("enrichment", {})
    stuck = data.get("stuck_issues", [])

    lines = []

    # --- YAML frontmatter ---
    lines.append("---")
    lines.append(f"project: {project}")
    lines.append(f"date: {date}")
    lines.append(f"red_flags: {summary.get('red_flags', 0)}")
    lines.append(f"yellow_flags: {summary.get('yellow_flags', 0)}")
    lines.append("metrics:")
    for key in ["clt", "lt", "ct", "fe"]:
        m = metrics.get(key, {})
        lines.append(f"  {key}: {m.get('avg', 'null')}")
    tp = metrics.get("throughput", {}).get("by_week", {})
    tp_vals = list(tp.values())
    lines.append(f"  throughput: {tp_vals[-1] if tp_vals else 'null'}")
    lines.append(f"  wip: {metrics.get('wip', {}).get('total', 'null')}")
    lines.append(f"  review_queue_hours: {metrics.get('review_queue', {}).get('avg_hours', 'null')}")
    lines.append(f"  defect_containment: {metrics.get('defect_containment', {}).get('pct', 'null')}")
    if bottleneck:
        lines.append(f"bottleneck: {bottleneck['stage']}")
    lines.append("---")
    lines.append("")

    # --- Header ---
    lines.append(f"# Digest | {project} | {date}")
    lines.append("")

    # --- RED FLAGS ---
    red_alerts = [a for a in alerts if a["level"] == "red"]
    if red_alerts:
        lines.append("## RED FLAGS")
        lines.append("")
        for a in red_alerts:
            lines.append(f"- **{a['message']}**")
        lines.append("")

    # --- YELLOW FLAGS ---
    yellow_alerts = [a for a in alerts if a["level"] == "yellow"]
    if yellow_alerts:
        lines.append("## ATTENTION")
        lines.append("")
        for a in yellow_alerts:
            lines.append(f"- {a['message']}")
        lines.append("")

    # --- BOTTLENECK ---
    if bottleneck:
        lines.append("## BOTTLENECK")
        lines.append("")
        ev = bottleneck.get("evidence", {})
        lines.append(f"**{bottleneck['stage']}** (score: {bottleneck['score']})")
        lines.append(f"- Avg time: {ev.get('avg_hours', '?')}h | p85: {ev.get('p85_hours', '?')}h")
        lines.append(f"- Share of cycle time: {ev.get('pct_of_cycle_time', '?')}%")
        lines.append(f"- WIP in this stage: {ev.get('wip_count', '?')}")
        stuck_in = ev.get("stuck_issues", [])
        if stuck_in:
            lines.append(f"- Stuck issues: {', '.join(stuck_in)}")
        runner = bottleneck.get("runner_up")
        if runner:
            lines.append(f"- Runner-up: {runner['stage']} ({runner['pct_of_cycle_time']}% of CT)")
        lines.append("")

    # --- METRICS SUMMARY ---
    lines.append("## METRICS")
    lines.append("")
    lines.append("| Metric | Avg | Median | p85 | Sample |")
    lines.append("|--------|-----|--------|-----|--------|")
    for key, label in [("clt", "Customer Lead Time (days)"), ("lt", "Lead Time (days)"),
                       ("ct", "Cycle Time (days)"), ("fe", "Flow Efficiency (%)")]:
        m = metrics.get(key, {})
        lines.append(f"| {label} | {m.get('avg', '-')} | {m.get('median', '-')} | {m.get('p85', '-')} | {m.get('sample_size', 0)} |")

    # Throughput
    tp_vals = list(metrics.get("throughput", {}).get("by_week", {}).values())
    if tp_vals:
        recent = tp_vals[-1]
        avg_tp = round(sum(tp_vals[-4:]) / min(4, len(tp_vals)), 1)
        lines.append(f"| Throughput (tasks/week) | {avg_tp} | - | - | last: {recent} |")

    # Review queue
    rq = metrics.get("review_queue", {})
    lines.append(f"| Review Queue (hours) | {rq.get('avg_hours', '-')} | - | {rq.get('p85_hours', '-')} | {rq.get('sample_size', 0)} |")

    # Defect containment
    dc = metrics.get("defect_containment", {})
    lines.append(f"| Defect Containment (%) | {dc.get('pct', '-')} | - | - | {dc.get('total', 0)} bugs |")
    lines.append("")

    # --- WIP ---
    wip = metrics.get("wip", {})
    if wip.get("total", 0) > 0:
        lines.append("## WIP")
        lines.append("")
        lines.append(f"Total: **{wip['total']}**")
        lines.append("")
        if wip.get("by_status"):
            lines.append("| Status | Count |")
            lines.append("|--------|-------|")
            for status, count in wip["by_status"].items():
                lines.append(f"| {status} | {count} |")
            lines.append("")
        if wip.get("by_assignee"):
            lines.append("| Assignee | Count |")
            lines.append("|----------|-------|")
            for assignee, count in wip["by_assignee"].items():
                lines.append(f"| {assignee} | {count} |")
            lines.append("")

    # --- STUCK ISSUES ---
    if stuck:
        lines.append("## STUCK ISSUES")
        lines.append("")
        lines.append("| Issue | Status | Days | Assignee |")
        lines.append("|-------|--------|------|----------|")
        for s in sorted(stuck, key=lambda x: -x.get("days_in_status", 0)):
            lines.append(f"| {s['key']} | {s['status']} | {s['days_in_status']} | {s.get('assignee', '-')} |")
        lines.append("")

    # --- ENRICHMENT DETAILS ---
    if enrichment:
        lines.append("## ISSUE DETAILS (flagged)")
        lines.append("")
        for key, detail in enrichment.items():
            lines.append(f"### {key}: {detail.get('summary', '')}")
            lines.append(f"- Status: {detail.get('status', '?')} | Assignee: {detail.get('assignee', '?')} | SP: {detail.get('story_points', '?')}")

            # Links
            link_data = detail.get("links", {})
            blocked_by = link_data.get("blocked_by", [])
            blocks = link_data.get("blocks", [])
            if blocked_by:
                blk_str = ", ".join(f"{l['key']} ({l['status']})" for l in blocked_by)
                lines.append(f"- Blocked by: {blk_str}")
            if blocks:
                blk_str = ", ".join(f"{l['key']} ({l['status']})" for l in blocks)
                lines.append(f"- Blocks: {blk_str}")

            # Time in status
            tis = detail.get("status_history", {}).get("time_in_status", {})
            if tis:
                top3 = list(tis.items())[:3]
                tis_str = ", ".join(f"{s}: {h}h" for s, h in top3)
                lines.append(f"- Time in status (top): {tis_str}")

            # Comments
            comments = detail.get("comments_recent", [])
            if comments:
                lines.append(f"- Last comment ({comments[-1].get('author', '?')}, {comments[-1].get('date', '?')[:10]}):")
                lines.append(f"  > {comments[-1].get('text', '')[:200]}")

            lines.append("")

    # --- TOP SLOW CT ISSUES ---
    ct_slow = metrics.get("ct_top_slowest", [])
    if ct_slow:
        lines.append("## TOP SLOW CYCLE TIME")
        lines.append("")
        lines.append("| Issue | Days | Done |")
        lines.append("|-------|------|------|")
        for item in ct_slow[:5]:
            lines.append(f"| {item['key']} | {item['days']} | {item.get('done_at', '-')[:10]} |")
        lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Generate Markdown report from enriched JSON")
    parser.add_argument("--save", help="Directory to save report (creates YYYY-MM-DD.md)")
    args = parser.parse_args()

    raw = sys.stdin.read()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON input: {e}", file=sys.stderr)
        sys.exit(1)

    if "error" in data:
        print(f"Error: {data['error']}", file=sys.stderr)
        sys.exit(1)

    report = format_digest(data)

    if args.save:
        save_dir = Path(args.save)
        save_dir.mkdir(parents=True, exist_ok=True)
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        save_path = save_dir / f"{date_str}.md"
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"Report saved to: {save_path}", file=sys.stderr)

    print(report)


if __name__ == "__main__":
    main()
