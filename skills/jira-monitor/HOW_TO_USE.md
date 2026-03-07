# Jira Monitor — Kanban Metrics & Bottleneck Discovery

A multi-agent Claude Code skill that monitors Kanban flow metrics from Jira data stored in DuckDB.

## What it does

- Syncs data from Jira API into DuckDB (delta or full sync)
- Calculates 13+ flow metrics: CLT, Lead Time, Cycle Time, Flow Efficiency, Throughput, WIP, Review Queue, Defect Containment, Bug Velocity, Sprint Scope Change, Estimate Accuracy, Workload Balance
- Detects worsening trends using SPC-inspired analysis
- Finds the #1 bottleneck with evidence
- Enriches flagged issues with comments, blocking links, and status history
- Presents results interactively with coaching recommendations

## Architecture

```
Main Agent (coach, dialog with user)
  |
  +-- Worker Subagent (runs pipeline, reads large JSON, returns summary)
       |
       +-- sync.py        Jira API -> DuckDB
       +-- discover.py    Scan DB, build project profile
       +-- setup_config.py  Map statuses to categories
       +-- collect.py     SQL queries, raw metrics
       +-- analyze.py     Trends, alerts, bottleneck scoring
       +-- enrich.py      Comments, links, history for flagged issues
       +-- report.py      Markdown digest
```

## Three Flows

1. **Onboarding** (new project): credentials -> sync -> discovery -> context interview -> first analysis
2. **Discovery** (refresh): "what changed?" -> delta scan -> update config
3. **Monitor** (daily): sync -> pipeline -> executive summary -> offer slices -> deep dive

## Installation

1. Copy this folder to `~/.claude/skills/jira-monitor/`
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and fill in your Jira credentials:
   ```bash
   cp .env.example .env
   ```
4. Run `/jira-monitor` in Claude Code

## Prerequisites

- Python 3.10+
- DuckDB database with Jira data (the skill can create it via `sync.py`)
- Jira API access (Server/DC PAT or Cloud API Token)

## Usage

In Claude Code, just say:
```
/jira-monitor покажи метрики PILOT
/jira-monitor как дела у команды?
/jira-monitor подключи проект NEWPROJECT
```

Or use trigger words (the skill auto-activates):
```
какие алерты сейчас?
покажи дайджест
есть ли red flags?
```

## Configuration Files

- `flow_config.json` — status mapping (backlog/active/waiting/done), WIP statuses, thresholds
- `project_profile.json` — project-specific: bug types, containment labels, alert thresholds, team context
- `.env` — Jira API credentials (never committed)

## Alert Types

### Core (v1)
- CLT/LT/CT trend worsening
- Flow Efficiency / Throughput declining
- Review Queue too long
- WIP too high

### Extended (v2)
- Bug Ratio too high
- Bug Velocity rising
- Bug Net Flow (backlog growing)
- Time-to-Fix too slow
- Defect Containment too low
- Sprint Scope Change too high
- Estimate Accuracy (systematic underestimation)
- Workload Overload (>40h/week)

## Security

- All scripts use `read_only=True` — they never modify the database
- `.env` is in `.gitignore` — credentials are never committed
- API tokens are read from environment variables only
