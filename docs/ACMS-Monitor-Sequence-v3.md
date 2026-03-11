# ACMS Monitor — What It Shows and How It Works

**Mind Over Metadata LLC — Peter Heller**
`QCadjunct/acms-langgraph-poc` · `ui/acms_monitor.py`

---

## What Is the ACMS Monitor?

The ACMS Monitor is a live dashboard that watches **AI agent pipelines execute in real time**.
It is the modern equivalent of the DEC ACMS console — the operator screen that showed every
task running on a VAX cluster, its status, its step sequence, and whether it succeeded or failed.

The pipeline it monitors is called **PIPELINE_ACMS_PROOF** — a proof-of-concept that demonstrates
all four agent types working together inside a LangGraph workflow:

| Agent Type | What It Does |
|---|---|
| **AGENT** | Single AI call — one skill, one system prompt, one answer |
| **SUBAGENT** | A mini-pipeline — runs two skills back-to-back as one atomic unit |
| **TEAM** | Parallel workers — three AI agents run simultaneously, results merged |
| **PYTHON** | Pure code — no AI, deterministic, but fully audited like the others |

---

## The Pipeline This Monitor Watches

### PIPELINE_ACMS_PROOF — Four Steps, Four Agent Types

```mermaid
stateDiagram-v2
    direction TB

    [*] --> DATA_EXTRACT : Task submitted

    state DATA_EXTRACT {
        direction TB
        [*] --> calling_llm : AGENT node fires
        calling_llm --> result : LLM responds
        result --> [*]
    }

    DATA_EXTRACT --> check_extract : StepStatus?

    state check_extract <<choice>>
    check_extract --> RETRY_extract : FAILED  (attempt < 3)
    check_extract --> VALIDATION_COMPOSITE : COMPLETED
    check_extract --> ABORT : FAILED (attempt = 3)

    RETRY_extract --> DATA_EXTRACT : retry same step

    state VALIDATION_COMPOSITE {
        direction TB
        [*] --> FORMAT_CHECK : SUBAGENT fires
        FORMAT_CHECK --> SCHEMA_CHECK : format pass
        SCHEMA_CHECK --> [*] : schema pass
    }

    VALIDATION_COMPOSITE --> check_validation : StepStatus?

    state check_validation <<choice>>
    check_validation --> RETRY_data : FAILED — re-extract
    check_validation --> TEAM_ENRICH : COMPLETED

    RETRY_data --> DATA_EXTRACT : retry from Step 1

    state TEAM_ENRICH {
        direction LR
        [*] --> researcher   : fan-out
        [*] --> synthesizer  : fan-out
        [*] --> formatter    : fan-out
        researcher  --> WFLAND : member result
        synthesizer --> WFLAND : member result
        formatter   --> WFLAND : member result
        WFLAND --> [*] : aggregated
    }

    TEAM_ENRICH --> check_enrich : StepStatus?

    state check_enrich <<choice>>
    check_enrich --> INFRA_PYTHON_PERSIST : COMPLETED or PARTIAL
    check_enrich --> INFRA_PYTHON_PERSIST : FAILED (SKIP_STEP — non-blocking)

    state INFRA_PYTHON_PERSIST {
        direction TB
        [*] --> write_to_db : PYTHON node — no LLM
        write_to_db --> [*] : rows committed
    }

    INFRA_PYTHON_PERSIST --> check_persist : StepStatus?

    state check_persist <<choice>>
    check_persist --> SESSION_COMPLETE : COMPLETED
    check_persist --> SESSION_FAILED   : FAILED (FAIL_TASK — blocking)

    SESSION_COMPLETE --> [*] : WorkspaceState accumulated ✓
    SESSION_FAILED   --> [*] : WorkspaceState accumulated ✗
    ABORT            --> [*] : Max retries exceeded ✗
```

### The Four Execution Scenarios in the Mock Data

| Scenario | What Happens | Final Status |
|---|---|---|
| **Happy path** | All four steps complete first try | ✅ COMPLETED |
| **Retry then success** | Step 1 fails, retries twice, then succeeds | ✅ COMPLETED |
| **Team partial fail** | One of three team members times out — pipeline continues | ✅ COMPLETED |
| **Hard failure** | Step 4 (database write) fails — entire session aborted | ❌ FAILED |

### WorkspaceState — The Accumulator

Every step appends its result to the **WorkspaceState**. It never overwrites — it only grows.
This is the D⁴ invariant: `Annotated[list[WorkspaceEntry], operator.add]`.
The Monitor reads this accumulated state to build every table and diagram you see.

---

## What Each Panel Shows

### Panel 1 — Audit Trail Explorer

**Plain English:** "Show me every session that ran and every step inside it."

This panel is the session logbook. Every time the pipeline runs — whether it succeeded,
failed, or retried — a session record is written. The Audit Trail lets you:

- **Filter sessions** by outcome (completed / failed) and operating mode
- **Filter steps** by agent type (Agent, SubAgent, Team, Python)
- **See aggregations** — which agent type takes longest on average, how many steps retried

Think of it as the VAX operator console audit log. If something went wrong, this is where
you find it.

| Table | What It Shows |
|---|---|
| **Sessions** | One row per pipeline run — ID, status, duration, error count |
| **Step Entries** | One row per step per session — which skill, which agent, how long, retry count |
| **Duration by Agent** | Average milliseconds per agent type — where is time being spent? |
| **Status Counts** | How many steps completed / failed / retried across all sessions |

---

### Panel 2 — Registry Analytics

**Plain English:** "Show me what skills and tasks are registered and their version history."

The Registry is the ACMS equivalent of the Application Definition File (ADF) —
the catalogue of every skill (HOW to do something) and every task (WHAT to do).
Skills are versioned. Only one version is "current" at a time.

This panel lets you:

- **Filter by domain** — see only Data skills, or only Validation skills
- **Toggle current-only** — hide historical versions, show only what is active
- **Track version history** — how many times has a skill been updated?

| Table | What It Shows |
|---|---|
| **Domain summary** | How many skills exist per domain, how many are current |
| **Version history** | Each skill FQSN and how many versions exist in the registry |
| **Skills** | Full skill catalogue — FQSN path, version, hash, valid-from/valid-to dates |
| **Tasks** | Task registry — PIPELINE_ACMS_PROOF and its step definitions |

---

### Panel 3 — Pipeline Dashboard

**Plain English:** "Show me one session's complete execution as a diagram."

Pick any session from the dropdown and this panel draws its execution graph —
the actual path the pipeline took through the four steps, color-coded by outcome.
Below the graph is a step-by-step table with timing, retry counts, and error messages.

This is the ACMS Monitor's main screen — the equivalent of watching a task execute
in the VAX ACMS console in real time, step by step.

**Execution graph color key:**

| Color | Meaning |
|---|---|
| 🟢 Green | Step completed successfully |
| 🔴 Red | Step failed |
| 🟡 Yellow | Step was retried |
| ⬜ Grey | Step was skipped (non-blocking) |

**Node icons:**

| Icon | Agent Type | Meaning |
|---|---|---|
| `A` | AGENT | Single LLM call |
| `S` | SUBAGENT | Sequential sub-pipeline |
| `T` | TEAM | Parallel fan-out workers |
| `P` | PYTHON | Deterministic code, no LLM |

---

## Cell Dependency Map — How Marimo Wires the Dashboard Together

```mermaid
graph TD
    subgraph BOOT["① Bootstrap"]
        MO["_mo\nimport marimo as mo"]
        IM["_imports\nduckdb · polars · pandas\nloader functions"]
    end

    subgraph GLOBAL["② Global Layer"]
        HD["_header\ntitle + data-source callout\nLive PostgreSQL or Mock"]
        CT["_controls\nCREATE\nSessions slider · Seed · Refresh"]
        LD["_load_data\nREAD slider + seed + button\nload_sessions · load_registry\nsession_df · entry_df · skill_df"]
        KP["_kpis\n6 summary stats\nSessions · Completed · Failed\nRetries · Avg ms · Error Rate"]
    end

    subgraph PANEL1["③ Panel 1 — Audit Trail"]
        P1W["_p1_widgets\nCREATE\nStatus filter · Mode filter\nAgent type filter"]
        P1D["_p1_data\nREAD filters\nPolars filter sessions + entries\nDuckDB avg duration, status counts"]
        P1R["_panel1\nSessions table\nStep Entries table\nDuration by Agent · Status Counts"]
    end

    subgraph PANEL2["④ Panel 2 — Registry"]
        P2W["_p2_widgets\nCREATE\nDomain filter · Current-only toggle"]
        P2D["_p2_data\nREAD filters\nPolars filter skill registry\nDuckDB domain summary, versions"]
        P2R["_panel2\nDomain summary · Version history\nSkills table · Tasks table"]
    end

    subgraph PANEL3["⑤ Panel 3 — Pipeline"]
        P3W["_p3_widget\nCREATE\nSession selector dropdown"]
        P3R["_panel3\nREAD selected session\nBuild Mermaid execution graph\nSession cards · Step detail table"]
    end

    subgraph ASSEMBLY["⑥ Final Assembly"]
        AS["_assemble\nmo.ui.tabs\nAudit Trail · Registry · Pipeline"]
        RN["_render\nmo.vstack\nHeader + Controls + KPIs + Tabs\nDOM OUTPUT"]
    end

    MO --> IM
    IM --> HD & CT & LD & KP & P1W & P1D & P2W & P2D & P3W & P3R

    HD --> RN
    CT --> LD
    CT --> RN
    LD --> KP
    LD --> P1D
    LD --> P2W & P2D
    LD --> P3W & P3R
    KP --> RN

    P1W --> P1D & P1R
    P1D --> P1R
    P1R --> AS

    P2W --> P2D & P2R
    P2D --> P2R
    P2R --> AS

    P3W --> P3R
    P3R --> AS
    AS  --> RN

    style MO   fill:#dfe6e9,stroke:#636e72,color:#000
    style IM   fill:#dfe6e9,stroke:#636e72,color:#000
    style HD   fill:#74b9ff,stroke:#0984e3,color:#000
    style CT   fill:#a29bfe,stroke:#6c5ce7,color:#000
    style LD   fill:#fd79a8,stroke:#e84393,color:#000
    style KP   fill:#55efc4,stroke:#00b894,color:#000
    style P1W  fill:#a29bfe,stroke:#6c5ce7,color:#000
    style P1D  fill:#fd79a8,stroke:#e84393,color:#000
    style P1R  fill:#55efc4,stroke:#00b894,color:#000
    style P2W  fill:#a29bfe,stroke:#6c5ce7,color:#000
    style P2D  fill:#fd79a8,stroke:#e84393,color:#000
    style P2R  fill:#55efc4,stroke:#00b894,color:#000
    style P3W  fill:#a29bfe,stroke:#6c5ce7,color:#000
    style P3R  fill:#fd79a8,stroke:#e84393,color:#000
    style AS   fill:#ffeaa7,stroke:#fdcb6e,color:#000
    style RN   fill:#2d3436,stroke:#000,color:#fff
```

**Color key:** 🔘 Grey = bootstrap · 🔵 Blue = render-only · 🟣 Purple = CREATE widget
🔴 Pink = READ + compute · 🟢 Green = panel render · 🟡 Yellow = assemble · ⬛ Black = DOM

**Marimo reactive rule:** Move the Sessions slider and only the pink and green cells
downstream of `_load_data` re-execute. Change a Panel 1 filter and only `_p1_data`
and `_panel1` re-run. Nothing else executes. This is the DAG.

---

## Sequence Diagrams — 8-Part Chain

> Step numbers run continuously 1 through 15 across all 8 diagrams.
> **🔁** marks a Marimo reactive re-fire — only the subgraph downstream of the change re-runs.

---

### SD-1 of 8 — Bootstrap

```mermaid
sequenceDiagram
    autonumber
    participant MO  as _mo
    participant IM  as _imports
    participant LDR as loader.py

    note over MO: Marimo starts here — no dependencies, always runs first
    MO ->> MO  : import marimo as mo
    MO -->> IM : mo  [available to every downstream cell]

    note over IM: Loads every library and data function the notebook needs
    IM ->> IM  : import duckdb, polars as pl, pandas as pd
    IM ->> LDR : resolve load_sessions, load_registry
    IM ->> LDR : resolve sessions_to_df, entries_to_df
    IM ->> LDR : resolve skill_records_to_df, using_live_db
    LDR -->> IM: all 6 loader functions ready
    IM -->> IM : exports everything downstream cells need

    note over IM: Continue to SD-2
```

---

### SD-2 of 8 — Header & Controls

```mermaid
sequenceDiagram
    autonumber
    participant IM  as _imports
    participant LDR as loader.py
    participant HD  as _header
    participant CT  as _controls
    participant U   as User

    note over HD: Checks whether we are watching real data or mock data
    IM -->> HD : mo, using_live_db
    HD ->> LDR : using_live_db()
    LDR ->> LDR : check ACMS_DATABASE_URL environment variable
    LDR -->> HD : False — mock mode active
    HD ->> HD  : show yellow "Mock data" banner
    HD ->> HD  : build page title + subtitle + banner → header

    note over CT: Creates the three controls — values are NOT read here
    note over CT: Marimo rule — CREATE and READ must be in separate cells
    IM -->> CT : mo
    CT ->> CT  : Sessions slider  [5 to 50, default 10]
    CT ->> CT  : Seed number input  [1 to 999, default 42]
    CT ->> CT  : Refresh button  [green]
    CT -->> CT : exports: session_count, mock_seed, refresh_btn

    note over U,CT: Controls appear on screen — user can now interact
    note over CT: Continue to SD-3
```

---

### SD-3 of 8 — Data Load

```mermaid
sequenceDiagram
    autonumber
    participant U   as User
    participant CT  as _controls
    participant LD  as _load_data
    participant LDR as loader.py

    note over LD: The reactive hub — every panel depends on this cell
    note over LD: Any control change causes this entire cell to re-run

    CT -->> LD : session_count, mock_seed, refresh_btn

    U   ->> CT : move Sessions slider to 20
    CT -->> LD : 🔁 Marimo detects the change and re-fires _load_data

    LD ->> LD  : READ refresh_btn.value   [any click also triggers reload]
    LD ->> LD  : READ session_count.value → 20 sessions requested
    LD ->> LD  : READ mock_seed.value     → seed 42 for reproducible data

    LD ->> LDR : load_sessions(count=20, seed=42)
    LDR ->> LDR: generate 20 sessions mixing the 4 scenarios
    LDR -->> LD: sessions  [list of session dicts with step entries inside]

    LD ->> LDR : load_registry(seed=42)
    LDR ->> LDR: generate skill catalogue + task definitions
    LDR -->> LD: registry  [dict]

    LD ->> LDR : sessions_to_df(sessions) → session_df  [one row per session]
    LD ->> LDR : entries_to_df(sessions)  → entry_df   [one row per step]
    LD ->> LDR : skill_records_to_df(registry) → skill_df [one row per skill version]

    LD -->> LD : exports: sessions, registry, session_df, entry_df, skill_df
    note over LD: 🔁 All panels now re-execute with fresh data
    note over LD: Continue to SD-4
```

---

### SD-4 of 8 — KPI Bar

```mermaid
sequenceDiagram
    autonumber
    participant LD  as _load_data
    participant KP  as _kpis
    participant U   as User

    note over KP: Pure calculation — counts and averages from the session data
    note over KP: 🔁 Re-runs every time _load_data produces new data

    LD -->> KP : session_df, entry_df, pl, mo

    KP ->> KP  : count all sessions                              → 20
    KP ->> KP  : count where status = completed                  → 18
    KP ->> KP  : count where is_failed = True                   → 2
    KP ->> KP  : count step entries where status = retried       → 4
    KP ->> KP  : average of total_ms column                     → 6700 ms
    KP ->> KP  : failed divided by total times 100              → 10%

    KP ->> KP  : assemble 6 stat boxes in a horizontal row
    KP -->> U  : KPI bar visible at top of dashboard

    note over KP: Continue to SD-5
```

---

### SD-5 of 8 — Panel 1: Audit Trail Explorer

```mermaid
sequenceDiagram
    autonumber
    participant U   as User
    participant LD  as _load_data
    participant P1W as _p1_widgets
    participant P1D as _p1_data
    participant DK  as DuckDB
    participant P1  as _panel1

    note over P1W: Creates the three filter controls for the Audit Trail panel
    P1W ->> P1W : Status dropdown     [all / completed / failed]
    P1W ->> P1W : Mode dropdown       [all / maas / cloud / hybrid]
    P1W ->> P1W : Agent type picker   [all four selected by default]
    P1W -->> P1D: filter controls
    P1W -->> P1 : filter controls  [displayed in the panel header]

    note over P1D: 🔁 Re-fires when new data arrives OR any filter changes
    note over P1D: Only this panel re-runs — Panels 2 and 3 are not affected

    U   ->> P1W : set Status filter to "failed"
    P1W -->> P1D: 🔁 Marimo re-fires _p1_data
    LD  -->> P1D: session_df, entry_df

    P1D ->> P1D : keep only failed sessions
    P1D ->> P1D : keep only selected agent types in step entries

    alt step entries exist
        P1D ->> DK : open DuckDB connection
        P1D ->> DK : load entry_df as in-memory table
        P1D ->> DK : calculate average duration per agent type
        P1D ->> DK : count steps per status
        P1D ->> DK : close connection
    else no entries
        P1D ->> P1D : return empty tables with correct column names
    end

    P1D -->> P1 : four filtered DataFrames ready to display

    P1  ->> P1  : Sessions table      [one row per session]
    P1  ->> P1  : Step Entries table  [one row per step]
    P1  ->> P1  : Duration by Agent   [avg ms per agent type]
    P1  ->> P1  : Status Counts       [completed / failed / retried totals]
    P1  -->> U  : Audit Trail tab updated

    note over P1: Continue to SD-6
```

---

### SD-6 of 8 — Panel 2: Registry Analytics

```mermaid
sequenceDiagram
    autonumber
    participant U   as User
    participant LD  as _load_data
    participant P2W as _p2_widgets
    participant P2D as _p2_data
    participant DK  as DuckDB
    participant P2  as _panel2

    note over P2W: Builds filter controls — reads domain names from the skill registry
    LD  -->> P2W: skill_df
    P2W ->> P2W : read distinct domain names from skill_df
    P2W ->> P2W : Domain multi-select  [one option per domain found]
    P2W ->> P2W : Current versions only checkbox  [checked by default]
    P2W -->> P2D: filter controls
    P2W -->> P2 : filter controls

    note over P2D: 🔁 Re-fires when new data arrives OR any filter changes
    note over P2D: Only Panel 2 re-runs — Panels 1 and 3 are not affected

    U   ->> P2W : uncheck "Current versions only"
    P2W -->> P2D: 🔁 Marimo re-fires _p2_data
    LD  -->> P2D: skill_df, registry

    P2D ->> P2D : if current-only checked → remove historical skill versions
    P2D ->> P2D : if domains selected → keep only those domains

    alt skill records exist
        P2D ->> DK : open DuckDB connection
        P2D ->> DK : load skill_df as in-memory table
        P2D ->> DK : count skills per domain, count current versions → domain summary
        P2D ->> DK : count versions per skill FQSN → version history
        P2D ->> DK : close connection
    else no skill records
        P2D ->> P2D : return empty tables with correct column names
    end

    P2D ->> P2D : read task records from registry
    P2D ->> P2D : convert to DataFrame for display
    P2D -->> P2 : four DataFrames ready

    P2  ->> P2  : Domain summary    [skills per domain, how many are current]
    P2  ->> P2  : Version history   [each skill, how many versions exist]
    P2  ->> P2  : Skills table      [full catalogue with FQSN, hash, dates]
    P2  ->> P2  : Tasks table       [PIPELINE_ACMS_PROOF step definitions]
    P2  -->> U  : Registry tab updated

    note over P2: Continue to SD-7
```

---

### SD-7 of 8 — Panel 3: Pipeline Dashboard

```mermaid
sequenceDiagram
    autonumber
    participant U   as User
    participant LD  as _load_data
    participant P3W as _p3_widget
    participant P3  as _panel3

    note over P3W: Builds the session picker dropdown from loaded sessions
    LD  -->> P3W: sessions list
    P3W ->> P3W : for each session build a readable label
    P3W ->> P3W : example label — "a1b2c3d4 | COMPLETED | 4381ms"
    P3W ->> P3W : set default to the first session in the list
    P3W ->> P3W : create dropdown with all session labels
    P3W -->> P3 : p3_select widget

    note over P3: 🔁 Re-fires when new data arrives OR user picks a different session
    note over P3: Only Panel 3 re-runs — Panels 1 and 2 are not affected

    U   ->> P3W : pick the failed session from the dropdown
    P3W -->> P3 : 🔁 Marimo re-fires _panel3

    P3  ->> P3  : look up the selected session from the sessions list
    P3  ->> P3  : read the session's step entries

    note over P3: Build the execution graph — one node per step
    P3  ->> P3  : START node
    loop for each step
        P3  ->> P3  : draw labeled node  [icon + skill name + status]
        P3  ->> P3  : color the node     [green=ok / red=fail / yellow=retry]
        P3  ->> P3  : connect to previous node with an arrow
        P3  ->> P3  : for team steps — draw sub-member nodes with dashed lines
    end
    P3  ->> P3  : END node

    P3  ->> P3  : 5 summary cards  [Session ID, Status, Duration, Steps, Errors]
    P3  ->> P3  : render execution graph via Mermaid
    P3  ->> P3  : step detail table  [step, type, skill, status, ms, retries, error]
    P3  -->> U  : Pipeline tab updated — selected session fully visualised

    note over P3: Continue to SD-8
```

---

### SD-8 of 8 — Assembly & Render

```mermaid
sequenceDiagram
    autonumber
    participant P1  as _panel1
    participant P2  as _panel2
    participant P3  as _panel3
    participant HD  as _header
    participant CT  as _controls
    participant KP  as _kpis
    participant AS  as _assemble
    participant RN  as _render
    participant U   as User / Browser

    note over AS: Wraps all three panels into a tabbed interface
    P1  -->> AS : Audit Trail panel content
    P2  -->> AS : Registry panel content
    P3  -->> AS : Pipeline panel content
    AS  ->> AS  : mo.ui.tabs — clicking a tab swaps the visible panel
    AS  -->> RN : tabs

    note over RN: Stacks everything into the final page
    HD  -->> RN : header  [title + data-source banner]
    CT  -->> RN : three controls  [slider, seed, refresh button]
    KP  -->> RN : six KPI stat boxes
    RN  ->> RN  : final page = header + controls + KPIs + tabs
    RN  -->> U  : ACMS Monitor — fully rendered

    note over U,RN: What happens when you interact

    U   ->> CT  : move Sessions slider from 10 to 20
    CT  -->> RN : 🔁 _load_data re-runs then all panels re-run
    CT  -->> RN :    header, controls, assembly, render do NOT re-run

    U   ->> AS  : change Status filter to "failed" in Panel 1
    AS  -->> RN : 🔁 only _p1_data and _panel1 re-run
    AS  -->> RN :    Panels 2 and 3, KPIs, header all unchanged

    U   ->> AS  : select a different session in Panel 3
    AS  -->> RN : 🔁 only _panel3 re-runs
    AS  -->> RN :    Panels 1 and 2, KPIs, header all unchanged
```

---

*© 2026 Mind Over Metadata LLC — Peter Heller. All rights reserved.*
