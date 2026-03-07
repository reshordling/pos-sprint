#!/usr/bin/env python3
"""
Stage 2: ANALYZE — Compute trends, alerts, and bottleneck scoring.

Usage:
    python collect.py PILOT | python analyze.py
    python analyze.py < collected.json

Reads JSON from stdin (output of collect.py), adds analysis layer, outputs enriched JSON.
"""

import json
import sys
from datetime import datetime


# --- Trend Detection (SPC-inspired) ---

def compute_baseline(weekly_values: list[float], window: int = 4) -> dict:
    """Compute rolling baseline from last N weeks."""
    if not weekly_values:
        return {"avg": None, "stddev": None, "values": []}
    recent = weekly_values[-window:] if len(weekly_values) >= window else weekly_values
    n = len(recent)
    avg = sum(recent) / n
    variance = sum((x - avg) ** 2 for x in recent) / n if n > 1 else 0
    stddev = variance ** 0.5
    return {"avg": round(avg, 2), "stddev": round(stddev, 2), "values": recent}


def detect_trend(weekly_values: list[float], higher_is_worse: bool = True) -> dict:
    """
    Detect trend direction and consecutive worsening.
    Returns: {direction, consecutive_worse, delta_pct, level}
    """
    if len(weekly_values) < 2:
        return {"direction": "insufficient_data", "consecutive_worse": 0,
                "delta_pct": 0, "level": "green"}

    baseline = compute_baseline(weekly_values[:-1])
    if baseline["avg"] is None or baseline["avg"] == 0:
        return {"direction": "no_baseline", "consecutive_worse": 0,
                "delta_pct": 0, "level": "green"}

    current = weekly_values[-1]
    delta_pct = round((current - baseline["avg"]) / abs(baseline["avg"]) * 100, 1)

    # Count consecutive worsening
    consecutive = 0
    for i in range(len(weekly_values) - 1, 0, -1):
        if higher_is_worse:
            if weekly_values[i] > weekly_values[i - 1]:
                consecutive += 1
            else:
                break
        else:
            if weekly_values[i] < weekly_values[i - 1]:
                consecutive += 1
            else:
                break

    # Determine if current value is "worse" than baseline
    is_worse = (delta_pct > 0) if higher_is_worse else (delta_pct < 0)
    abs_delta = abs(delta_pct)

    # Alert levels
    if is_worse and abs_delta > 15 and consecutive >= 3:
        level = "red"
    elif is_worse and (abs_delta > 15 or consecutive >= 2):
        level = "yellow"
    else:
        level = "green"

    direction = "worsening" if is_worse else "improving"
    if abs_delta < 5:
        direction = "stable"

    return {
        "direction": direction,
        "consecutive_worse": consecutive,
        "delta_pct": delta_pct if higher_is_worse else -delta_pct,
        "level": level,
        "current": round(current, 1),
        "baseline_avg": baseline["avg"],
    }


# --- Bottleneck Scoring ---

def score_bottleneck(avg_time_in_status: dict, wip_by_status: dict,
                     total_avg_ct_hours: float,
                     exclude_statuses: set[str] | None = None) -> list[dict]:
    """
    Rank statuses by bottleneck impact.
    Score = (avg_time / total_cycle_time) * sqrt(wip_count) * trend_multiplier
    """
    if not avg_time_in_status or total_avg_ct_hours <= 0:
        return []

    exclude = exclude_statuses or set()
    scores = []
    for status, data in avg_time_in_status.items():
        if status in exclude:
            continue
        avg_hours = data.get("avg_hours", 0)
        if avg_hours <= 0:
            continue
        time_share = avg_hours / total_avg_ct_hours
        wip_count = wip_by_status.get(status, 0)
        # sqrt to dampen WIP influence — 4 items is only 2x, not 4x
        wip_factor = max(1.0, wip_count ** 0.5)
        score = time_share * wip_factor
        scores.append({
            "status": status,
            "score": round(score, 3),
            "avg_hours": avg_hours,
            "p85_hours": data.get("p85_hours"),
            "pct_of_cycle_time": round(time_share * 100, 1),
            "wip_count": wip_count,
            "sample_size": data.get("sample_size", 0),
        })

    scores.sort(key=lambda x: -x["score"])
    return scores


# --- Main Analysis ---

def get_thresholds(data: dict) -> dict:
    """Get alert thresholds from profile or use defaults."""
    profile = data.get("profile", {})
    t = profile.get("thresholds", {})
    return {
        "bug_ratio_yellow": t.get("bug_ratio_yellow", 30),
        "bug_ratio_red": t.get("bug_ratio_red", 45),
        "scope_change_yellow": t.get("scope_change_yellow", 20),
        "scope_change_red": t.get("scope_change_red", 40),
        "estimate_ratio_yellow": t.get("estimate_ratio_yellow", 1.3),
        "estimate_ratio_red": t.get("estimate_ratio_red", 1.8),
        "ttf_median_yellow_days": t.get("ttf_median_yellow_days", 5),
        "ttf_median_red_days": t.get("ttf_median_red_days", 10),
        "overload_weekly_hours": t.get("overload_weekly_hours", 40),
        "containment_yellow_pct": t.get("containment_yellow_pct", 70),
        "containment_red_pct": t.get("containment_red_pct", 50),
        "review_queue_yellow_hours": t.get("review_queue_yellow_hours", 24),
        "review_queue_red_hours": t.get("review_queue_red_hours", 48),
        "wip_yellow": t.get("wip_yellow", 15),
        "wip_red": t.get("wip_red", 25),
    }


def analyze(data: dict) -> dict:
    """Add analysis layer to collected data."""
    by_week = data.get("by_week", {})
    metrics = data.get("metrics", {})
    th = get_thresholds(data)

    # Sort weeks chronologically
    sorted_weeks = sorted(by_week.keys())

    # Extract weekly series for trend detection
    weekly_series = {}
    for metric_name in ["clt", "lt", "ct", "fe"]:
        values = []
        for wk in sorted_weeks:
            v = by_week[wk].get(metric_name)
            if v is not None:
                values.append(v)
        weekly_series[metric_name] = values

    # Throughput weekly series
    tp_by_week = metrics.get("throughput", {}).get("by_week", {})
    tp_sorted = sorted(tp_by_week.keys())
    weekly_series["throughput"] = [tp_by_week[wk] for wk in tp_sorted]

    # Detect trends
    trends = {}
    # higher_is_worse for time metrics, lower_is_worse for efficiency/throughput
    trend_config = {
        "clt": True,
        "lt": True,
        "ct": True,
        "fe": False,       # lower flow efficiency is worse
        "throughput": False,  # lower throughput is worse
    }
    for metric_name, higher_is_worse in trend_config.items():
        series = weekly_series.get(metric_name, [])
        trends[metric_name] = detect_trend(series, higher_is_worse=higher_is_worse)

    # Build alerts list (red and yellow only)
    alerts = []
    metric_labels = {
        "clt": "Customer Lead Time",
        "lt": "Lead Time",
        "ct": "Cycle Time",
        "fe": "Flow Efficiency",
        "throughput": "Throughput",
    }
    metric_units = {
        "clt": "дн",
        "lt": "дн",
        "ct": "дн",
        "fe": "%",
        "throughput": "задач/нед",
    }

    for metric_name, trend in trends.items():
        if trend["level"] in ("red", "yellow"):
            current = trend.get("current", metrics.get(metric_name, {}).get("avg"))
            baseline = trend.get("baseline_avg")
            unit = metric_units.get(metric_name, "")
            label = metric_labels.get(metric_name, metric_name)
            delta_str = f"+{trend['delta_pct']}%" if trend["delta_pct"] > 0 else f"{trend['delta_pct']}%"

            message = f"{label}: {current}{unit}"
            if baseline:
                message += f" (baseline {baseline}{unit}, {delta_str})"
            if trend["consecutive_worse"] >= 2:
                message += f", ухудшение {trend['consecutive_worse']} нед подряд"

            alerts.append({
                "metric": metric_name,
                "level": trend["level"],
                "current": current,
                "baseline": baseline,
                "delta_pct": trend["delta_pct"],
                "consecutive_worse": trend["consecutive_worse"],
                "message": message,
            })

    # Review queue alert (check if avg_hours is high)
    rq = metrics.get("review_queue", {})
    if rq.get("avg_hours", 0) > th["review_queue_yellow_hours"]:
        alerts.append({
            "metric": "review_queue",
            "level": "yellow" if rq["avg_hours"] < th["review_queue_red_hours"] else "red",
            "current": rq["avg_hours"],
            "baseline": None,
            "delta_pct": None,
            "consecutive_worse": None,
            "message": f"Review Queue: среднее {rq['avg_hours']}ч (p85: {rq.get('p85_hours', '?')}ч)",
        })

    # WIP alert
    wip = metrics.get("wip", {})
    if wip.get("total", 0) > th["wip_yellow"]:
        alerts.append({
            "metric": "wip",
            "level": "yellow" if wip["total"] < th["wip_red"] else "red",
            "current": wip["total"],
            "baseline": None,
            "delta_pct": None,
            "consecutive_worse": None,
            "message": f"WIP: {wip['total']} задач в работе",
        })

    # --- Extended Metrics Alerts (v2) ---

    # Defect alerts
    defects = metrics.get("defects", {})
    if defects.get("total_bugs", 0) > 0:
        bug_ratio = defects.get("bug_ratio_pct", 0)
        if bug_ratio > th["bug_ratio_yellow"]:
            alerts.append({
                "metric": "bug_ratio",
                "level": "red" if bug_ratio > th["bug_ratio_red"] else "yellow",
                "current": bug_ratio,
                "baseline": None, "delta_pct": None, "consecutive_worse": None,
                "message": f"Bug Ratio: {bug_ratio}% задач — баги ({defects['total_bugs']} из {defects['total_bugs'] + defects.get('resolved_bugs', 0) - defects.get('open_bugs', 0)})",
            })

        ttf = defects.get("time_to_fix", {})
        if ttf.get("median_days", 0) > th["ttf_median_yellow_days"]:
            alerts.append({
                "metric": "time_to_fix",
                "level": "red" if ttf["median_days"] > th["ttf_median_red_days"] else "yellow",
                "current": ttf["median_days"],
                "baseline": None, "delta_pct": None, "consecutive_worse": None,
                "message": f"Time-to-Fix багов: медиана {ttf['median_days']}дн (p85: {ttf.get('p85_days', '?')}дн)",
            })

        # Bug velocity trend (rising creation rate)
        bug_created = defects.get("created_by_week", {})
        if len(bug_created) >= 3:
            bug_weeks = sorted(bug_created.keys())
            bug_values = [bug_created[w] for w in bug_weeks]
            bug_trend = detect_trend(bug_values, higher_is_worse=True)
            if bug_trend["level"] in ("red", "yellow"):
                alerts.append({
                    "metric": "bug_velocity",
                    "level": bug_trend["level"],
                    "current": bug_trend.get("current"),
                    "baseline": bug_trend.get("baseline_avg"),
                    "delta_pct": bug_trend["delta_pct"],
                    "consecutive_worse": bug_trend["consecutive_worse"],
                    "message": f"Bug Velocity: {bug_trend.get('current')} багов/нед (baseline {bug_trend.get('baseline_avg')}, {'+' if bug_trend['delta_pct'] > 0 else ''}{bug_trend['delta_pct']}%)",
                })

        # Bug net flow (created > resolved = growing backlog)
        bug_resolved = defects.get("resolved_by_week", {})
        if bug_created and bug_resolved:
            recent_weeks = sorted(bug_created.keys())[-4:]
            created_sum = sum(bug_created.get(w, 0) for w in recent_weeks)
            resolved_sum = sum(bug_resolved.get(w, 0) for w in recent_weeks)
            if created_sum > resolved_sum * 1.3 and created_sum > 4:
                alerts.append({
                    "metric": "bug_net_flow",
                    "level": "yellow" if created_sum < resolved_sum * 2 else "red",
                    "current": created_sum,
                    "baseline": resolved_sum,
                    "delta_pct": round((created_sum - resolved_sum) / max(resolved_sum, 1) * 100, 1),
                    "consecutive_worse": None,
                    "message": f"Bug Backlog растёт: создано {created_sum} vs закрыто {resolved_sum} за 4 нед",
                })

        # Defect containment
        containment = defects.get("containment", {})
        dc_pct = containment.get("containment_pct")
        if dc_pct is not None and dc_pct < th["containment_yellow_pct"]:
            alerts.append({
                "metric": "defect_containment",
                "level": "red" if dc_pct < th["containment_red_pct"] else "yellow",
                "current": dc_pct,
                "baseline": None, "delta_pct": None, "consecutive_worse": None,
                "message": f"Defect Containment: {dc_pct}% (pre-release: {containment.get('pre_release_count', 0)}, post: {containment.get('post_release_count', 0)})",
            })

    # Sprint scope change alerts
    sprint_scope = metrics.get("sprint_scope", {})
    if sprint_scope.get("has_sprints"):
        avg_scope = sprint_scope.get("avg_scope_change_pct", 0)
        if avg_scope > th["scope_change_yellow"]:
            alerts.append({
                "metric": "sprint_scope_change",
                "level": "red" if avg_scope > th["scope_change_red"] else "yellow",
                "current": avg_scope,
                "baseline": None, "delta_pct": None, "consecutive_worse": None,
                "message": f"Sprint Scope Change: {avg_scope}% задач добавлено после старта спринта (avg по {sprint_scope.get('total_sprints_found', '?')} спринтам)",
            })

    # Estimate accuracy alerts
    estimates = metrics.get("estimate_accuracy", {})
    if estimates.get("sample_size", 0) >= 5:
        avg_ratio = estimates.get("avg_ratio", 1.0)
        under_pct = estimates.get("under_estimated_pct", 0)
        if avg_ratio > th["estimate_ratio_yellow"]:
            alerts.append({
                "metric": "estimate_accuracy",
                "level": "red" if avg_ratio > th["estimate_ratio_red"] else "yellow",
                "current": avg_ratio,
                "baseline": 1.0,
                "delta_pct": round((avg_ratio - 1.0) * 100, 1),
                "consecutive_worse": None,
                "message": f"Estimate Accuracy: avg ratio {avg_ratio}x (факт/оценка), {under_pct}% задач недооценены",
            })

    # Worklog overload alerts
    worklogs = metrics.get("worklogs", {})
    overloaded_people = [p for p in worklogs.get("people", [])
                         if p.get("avg_weekly_hours", 0) > th["overload_weekly_hours"]]
    if overloaded_people:
        names = ", ".join(p["author"] for p in overloaded_people[:3])
        alerts.append({
            "metric": "workload_overload",
            "level": "red" if len(overloaded_people) >= 3 else "yellow",
            "current": len(overloaded_people),
            "baseline": None, "delta_pct": None, "consecutive_worse": None,
            "message": f"Перегрузка: {len(overloaded_people)} чел >40ч/нед ({names})",
        })

    # Sort alerts: red first, then yellow
    alert_order = {"red": 0, "yellow": 1}
    alerts.sort(key=lambda a: alert_order.get(a["level"], 2))

    # Bottleneck scoring — exclude done and backlog statuses
    avg_time = metrics.get("avg_time_in_status", {})
    wip_by_status = wip.get("by_status", {})
    total_ct_hours = (metrics.get("ct", {}).get("avg") or 1) * 24  # days to hours
    config = data.get("config", {})
    exclude_from_bottleneck = set(config.get("done_statuses", []) + config.get("backlog_statuses", []))
    bottleneck_ranking = score_bottleneck(avg_time, wip_by_status, total_ct_hours,
                                          exclude_statuses=exclude_from_bottleneck)

    # Identify primary bottleneck
    bottleneck = None
    if bottleneck_ranking:
        top = bottleneck_ranking[0]
        runner_up = bottleneck_ranking[1] if len(bottleneck_ranking) > 1 else None

        # Find stuck issues in bottleneck status
        stuck_in_bottleneck = [
            s for s in data.get("stuck_issues", [])
            if s.get("status") == top["status"]
        ]

        bottleneck = {
            "stage": top["status"],
            "score": top["score"],
            "evidence": {
                "avg_hours": top["avg_hours"],
                "p85_hours": top["p85_hours"],
                "pct_of_cycle_time": top["pct_of_cycle_time"],
                "wip_count": top["wip_count"],
                "stuck_issues": [s["key"] for s in stuck_in_bottleneck[:5]],
            },
            "runner_up": {
                "stage": runner_up["status"],
                "score": runner_up["score"],
                "pct_of_cycle_time": runner_up["pct_of_cycle_time"],
            } if runner_up else None,
        }

    # Summary
    summary = {
        "red_flags": sum(1 for a in alerts if a["level"] == "red"),
        "yellow_flags": sum(1 for a in alerts if a["level"] == "yellow"),
        "total_alerts": len(alerts),
        "bottleneck_stage": bottleneck["stage"] if bottleneck else None,
        "weeks_of_data": len(sorted_weeks),
        "bug_ratio_pct": metrics.get("defects", {}).get("bug_ratio_pct"),
        "avg_scope_change_pct": metrics.get("sprint_scope", {}).get("avg_scope_change_pct"),
        "estimate_ratio": metrics.get("estimate_accuracy", {}).get("avg_ratio"),
        "overloaded_count": len([p for p in metrics.get("worklogs", {}).get("people", []) if p.get("overloaded")]),
    }

    # Add analysis to data
    data["analysis"] = {
        "trends": trends,
        "alerts": alerts,
        "bottleneck": bottleneck,
        "bottleneck_ranking": bottleneck_ranking[:5],
        "summary": summary,
    }

    return data


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

    result = analyze(data)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
