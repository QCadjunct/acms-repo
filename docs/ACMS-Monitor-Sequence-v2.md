# ACMS Monitor — Cell Execution Sequence Diagrams

**Mind Over Metadata LLC — Peter Heller**
`QCadjunct/acms-langgraph-poc` · `ui/acms_monitor.py`

> Four diagrams, each a link in the chain. Read top to bottom.
> **Diagram 1 → 2 → 3 → 4** — each picks up where the previous left off.

---

## Diagram 1 of 4 — Bootstrap & Data Load

*Covers: `_mo`, `_imports`, `_header`, `_controls`, `_load_data`, `_kpis`*

```mermaid
sequenceDiagram
    autonumber

    participant U   as 👤 User
    participant MO  as _mo
    participant IM  as _imports
    participant LDR as loader.py
    participant HD  as _header
    participant CT  as _controls
    participant LD  as _load_data
    participant KP  as _kpis

    note over MO,IM: STEP 1 — Bootstrap

    MO  ->> MO  : import marimo as mo
    MO  -->> IM : mo

    IM  ->> IM  : import duckdb, polars as pl, pandas as pd
    IM  ->> LDR : resolve load_sessions, load_registry
    IM  ->> LDR : resolve sessions_to_df, entries_to_df
    IM  ->> LDR : resolve skill_records_to_df, using_live_db
    LDR -->> IM : all loader functions imported

    note over HD: STEP 2 — Header

    IM  -->> HD : mo, using_live_db
    HD  ->> LDR : using_live_db()
    LDR -->> HD : bool — checks ACMS_DATABASE_URL env var
    HD  ->> HD  : select callout kind (success / warn)
    HD  ->> HD  : mo.vstack([title, subtitle, callout])
    HD  -->> KP : header  ➜ flows to _render (Diagram 4)

    note over CT: STEP 3 — Controls (CREATE only — no .value here)

    IM  -->> CT : mo
    CT  ->> CT  : mo.ui.slider(5,50,10)  → session_count
    CT  ->> CT  : mo.ui.number(1,999,42) → mock_seed
    CT  ->> CT  : mo.ui.button("Refresh")→ refresh_btn
    CT  -->> LD : session_count, mock_seed, refresh_btn
    CT  -->> KP : (indirectly via session_df)

    note over LD,LDR: STEP 4 — Data Load (READ .value)

    U   ->> CT  : adjust slider / seed / click Refresh
    CT  -->> LD : DAG fires _load_data
    LD  ->> LD  : READ refresh_btn.value   [reactive trigger]
    LD  ->> LD  : READ session_count.value → _n
    LD  ->> LD  : READ mock_seed.value     → _s
    LD  ->> LDR : load_sessions(count=_n, seed=_s)
    LDR -->> LD : sessions  [list of session dicts]
    LD  ->> LDR : load_registry(seed=_s)
    LDR -->> LD : registry  [dict of skill/task records]
    LD  ->> LDR : sessions_to_df(sessions)
    LDR -->> LD : session_df  [Polars DataFrame]
    LD  ->> LDR : entries_to_df(sessions)
    LDR -->> LD : entry_df   [Polars DataFrame]
    LD  ->> LDR : skill_records_to_df(registry)
    LDR -->> LD : skill_df   [Polars DataFrame]
    LD  -->> KP : session_df, entry_df
    LD  -->> KP : (skill_df → Panel 2, sessions → Panel 3)

    note over KP: STEP 5 — KPI Bar

    KP  ->> KP  : pl.col("status")=="completed"  → _comp
    KP  ->> KP  : pl.col("is_failed")==True       → _fail
    KP  ->> KP  : pl.col("status")=="retried"     → _ret
    KP  ->> KP  : session_df["total_ms"].mean()   → _avg
    KP  ->> KP  : (_fail/_tot*100)                → _rate
    KP  ->> KP  : mo.hstack([6 × mo.stat()])
    KP  -->> KP : kpis  ➜ flows to _render (Diagram 4)

    note over KP: ➜ Continue to Diagram 2 — Panel 1
```

---

## Diagram 2 of 4 — Panel 1: Audit Trail Explorer

*Picks up after `_load_data`. Covers: `_p1_widgets`, `_p1_data`, `_panel1`*

```mermaid
sequenceDiagram
    autonumber

    participant U   as 👤 User
    participant LD  as _load_data
    participant P1W as _p1_widgets
    participant P1D as _p1_data
    participant DK  as DuckDB
    participant P1  as _panel1

    note over P1W: STEP 6 — Panel 1 Widgets (CREATE only)

    P1W ->> P1W : mo.ui.dropdown(["all","completed","failed"]) → p1_status
    P1W ->> P1W : mo.ui.dropdown(["all","maas","cloud","hybrid"]) → p1_mode
    P1W ->> P1W : mo.ui.multiselect(agent types, all selected) → p1_agents
    P1W -->> P1D: p1_status, p1_mode, p1_agents
    P1W -->> P1 : p1_status, p1_mode, p1_agents

    note over P1D,DK: STEP 7 — Panel 1 Data (READ .value + DuckDB)

    LD  -->> P1D: session_df, entry_df
    U   ->> P1W : change Status / Mode / Agent filter
    P1W -->> P1D: DAG fires _p1_data

    P1D ->> P1D : READ p1_status.value
    alt status != "all"
        P1D ->> P1D : session_df.filter(pl.col("status") == value)
    end
    P1D ->> P1D : READ p1_mode.value
    alt mode != "all"
        P1D ->> P1D : session_df.filter(pl.col("operating_mode") == value)
    end
    P1D ->> P1D : READ p1_agents.value → _sel
    alt entry_df not empty AND _sel not empty
        P1D ->> P1D : entry_df.filter(pl.col("agent_type").is_in(_sel))
    end

    P1D ->> P1D : Guard — entry_df.is_empty()?
    alt entry_df has rows
        P1D ->> DK  : duckdb.connect()
        P1D ->> DK  : register("e", entry_df.to_arrow())
        DK  -->> P1D: virtual table "e" ready
        P1D ->> DK  : SELECT agent_type, AVG(duration_ms), COUNT(*)
        DK  -->> P1D: _dur  [Pandas DataFrame]
        P1D ->> DK  : SELECT status, COUNT(*)
        DK  -->> P1D: _sts  [Pandas DataFrame]
        P1D ->> DK  : close()
    else entry_df empty
        P1D ->> P1D : empty DataFrames with named columns
    end

    P1D ->> P1D : _sdf.to_pandas() → p1_sess_pd
    P1D ->> P1D : _edf.to_pandas() → p1_entr_pd
    P1D -->> P1 : p1_sess_pd, p1_entr_pd, p1_dur_pd, p1_sts_pd

    note over P1: STEP 8 — Panel 1 Render

    P1  ->> P1  : mo.ui.table(p1_sess_pd, page_size=8)   [Sessions]
    P1  ->> P1  : mo.ui.table(p1_entr_pd, page_size=10)  [Step Entries]
    P1  ->> P1  : mo.ui.table(p1_dur_pd)                 [Duration by agent]
    P1  ->> P1  : mo.ui.table(p1_sts_pd)                 [Status counts]
    P1  ->> P1  : mo.vstack([heading, filters, sessions, entries, aggs])
    P1  -->> P1 : panel1  ➜ flows to _assemble (Diagram 4)

    note over P1: ➜ Continue to Diagram 3 — Panel 2
```

---

## Diagram 3 of 4 — Panel 2 & Panel 3

*Picks up after `_load_data`. Covers: `_p2_widgets`, `_p2_data`, `_panel2`, `_p3_widget`, `_panel3`*

```mermaid
sequenceDiagram
    autonumber

    participant U   as 👤 User
    participant LD  as _load_data
    participant P2W as _p2_widgets
    participant P2D as _p2_data
    participant DK  as DuckDB
    participant P2  as _panel2
    participant P3W as _p3_widget
    participant P3  as _panel3

    note over P2W: STEP 9 — Panel 2 Widgets (CREATE only)

    LD  -->> P2W: skill_df
    P2W ->> P2W : skill_df["domain"].unique().to_list() → _doms
    P2W ->> P2W : mo.ui.multiselect(options=_doms) → p2_domain
    P2W ->> P2W : mo.ui.checkbox(value=True)        → p2_current
    P2W -->> P2D: p2_domain, p2_current
    P2W -->> P2 : p2_domain, p2_current

    note over P2D,DK: STEP 10 — Panel 2 Data (READ .value + DuckDB)

    LD  -->> P2D: skill_df, registry
    U   ->> P2W : change domain / current filter
    P2W -->> P2D: DAG fires _p2_data

    P2D ->> P2D : READ p2_current.value
    alt current only
        P2D ->> P2D : skill_df.filter(pl.col("is_current")==True)
    end
    P2D ->> P2D : READ p2_domain.value → _sel2
    alt _sel2 not empty
        P2D ->> P2D : skill_df.filter(pl.col("domain").is_in(_sel2))
    end

    P2D ->> P2D : Guard — skill_df.is_empty()?
    alt skill_df has rows
        P2D ->> DK  : duckdb.connect()
        P2D ->> DK  : register("s", skill_df.to_arrow())
        DK  -->> P2D: virtual table "s" ready
        P2D ->> DK  : SELECT domain, COUNT(*), SUM(is_current)
        DK  -->> P2D: _dom  [Pandas DataFrame]
        P2D ->> DK  : SELECT fqsn, COUNT(*) versions
        DK  -->> P2D: _ver  [Pandas DataFrame]
        P2D ->> DK  : close()
    else skill_df empty
        P2D ->> P2D : empty DataFrames with named columns
    end

    P2D ->> P2D : registry.get("task_records") → _trecs
    P2D ->> P2D : pl.from_dicts(_trecs) [try/except → pd.DataFrame fallback]
    P2D ->> P2D : .to_pandas() → p2_skill_pd, p2_dom_pd, p2_ver_pd, p2_task_pd
    P2D -->> P2 : p2_skill_pd, p2_dom_pd, p2_ver_pd, p2_task_pd

    note over P2: STEP 11 — Panel 2 Render

    P2  ->> P2  : mo.ui.table(p2_dom_pd)    [Domain summary]
    P2  ->> P2  : mo.ui.table(p2_ver_pd)    [Version history]
    P2  ->> P2  : mo.ui.table(p2_skill_pd)  [Skills]
    P2  ->> P2  : mo.ui.table(p2_task_pd)   [Tasks]
    P2  ->> P2  : mo.vstack([heading, filters, domain+ver, skills, tasks])
    P2  -->> P2 : panel2  ➜ flows to _assemble (Diagram 4)

    note over P3W: STEP 12 — Panel 3 Widget (CREATE only)

    LD  -->> P3W: sessions
    P3W ->> P3W : Build _opts = {label_string: index_int} for each session
    P3W ->> P3W : _default = list(_opts.keys())[0]
    P3W ->> P3W : mo.ui.dropdown(options=_opts, value=_default) → p3_select
    P3W -->> P3 : p3_select

    note over P3: STEP 13 — Panel 3 Dashboard (READ .value + Mermaid)

    LD  -->> P3 : sessions
    U   ->> P3W : select session from dropdown
    P3W -->> P3 : DAG fires _panel3

    P3  ->> P3  : READ p3_select.value → _idx (int)
    P3  ->> P3  : sessions[_idx] → _s (session dict)
    P3  ->> P3  : Build _mermaid_diagram(_s):
    P3  ->> P3  :   graph TD header + START node
    loop for each entry in _s["entries"]
        P3  ->> P3  : node_id = f"n{step}_{entry_id[:4]}"
        P3  ->> P3  : label   = "{icon}:{skill}\n{status}"
        P3  ->> P3  : style   = fill:{color by status}
        P3  ->> P3  : prev --> node_id
        loop sub_entries (max 3)
            P3  ->> P3  : stadium node -.-> parent
        end
    end
    P3  ->> P3  : append END_NODE
    P3  ->> P3  : mo.hstack([5 × mo.stat()])   [session cards]
    P3  ->> P3  : mo.mermaid(diagram_string)
    P3  ->> P3  : mo.ui.table(step rows as DataFrame)
    P3  ->> P3  : mo.vstack([heading, select, cards, graph, table])
    P3  -->> P3 : panel3  ➜ flows to _assemble (Diagram 4)

    note over P3: ➜ Continue to Diagram 4 — Assembly & Render
```

---

## Diagram 4 of 4 — Assembly & Render

*Picks up after Panels 1, 2, 3. Covers: `_assemble`, `_render`*

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
    participant U   as 👤 User / Browser

    note over AS: STEP 14 — Assemble Tabs

    P1  -->> AS : panel1
    P2  -->> AS : panel2
    P3  -->> AS : panel3
    AS  ->> AS  : mo.ui.tabs({"Audit Trail": panel1,
    AS  ->> AS  :             "Registry":    panel2,
    AS  ->> AS  :             "Pipeline":    panel3})
    AS  -->> RN : tabs

    note over RN,U: STEP 15 — Final Render

    HD  -->> RN : header
    KP  -->> RN : kpis
    CT  -->> RN : session_count, mock_seed, refresh_btn
    AS  -->> RN : tabs

    RN  ->> RN  : mo.vstack([
    RN  ->> RN  :   header,
    RN  ->> RN  :   mo.hstack([session_count, mock_seed, refresh_btn]),
    RN  ->> RN  :   kpis,
    RN  ->> RN  :   tabs
    RN  ->> RN  : ])
    RN  -->> U  : 🖥️ Rendered ACMS Monitor page

    note over U,RN: Any widget change → Marimo re-fires only the affected subgraph
    U   ->> CT  : adjust session_count
    CT  -->> RN : DAG: _load_data → _kpis, _p1_data,<br/>_p2_data, _p3_widget re-execute
    U   ->> P1  : change p1_status filter
    P1  -->> RN : DAG: only _p1_data → _panel1 re-execute
```

---

## Cell Dependency Map

```mermaid
graph TD
    MO["_mo\nimport marimo as mo"]
    IM["_imports\nduckdb · polars · pandas\nloader functions"]
    HD["_header\nmo.vstack title + callout"]
    CT["_controls\nCREATE slider · number · button"]
    LD["_load_data\nREAD .value\nload_sessions · load_registry\nsessions_to_df · entries_to_df\nskill_records_to_df"]
    KP["_kpis\n6× mo.stat()"]
    P1W["_p1_widgets\nCREATE dropdown × 2\nmultiselect × 1"]
    P1D["_p1_data\nREAD .value\nPolars filter\nDuckDB aggregation"]
    P1["_panel1\nmo.ui.table × 4\nmo.vstack"]
    P2W["_p2_widgets\nCREATE multiselect\ncheckbox"]
    P2D["_p2_data\nREAD .value\nPolars filter\nDuckDB aggregation\npl.from_dicts"]
    P2["_panel2\nmo.ui.table × 4\nmo.vstack"]
    P3W["_p3_widget\nCREATE dropdown\nfirst key as default"]
    P3["_panel3\nREAD .value\nBuild Mermaid diagram\nmo.stat × 5\nmo.ui.table"]
    AS["_assemble\nmo.ui.tabs"]
    RN["_render\nmo.vstack\nFinal DOM output"]

    MO --> IM
    IM --> HD
    IM --> CT
    IM --> LD
    IM --> KP
    IM --> P1W
    IM --> P1D
    IM --> P2W
    IM --> P2D
    IM --> P3W
    IM --> P3

    HD --> RN
    CT --> LD
    CT --> RN

    LD --> KP
    LD --> P1D
    LD --> P2W
    LD --> P2D
    LD --> P3W
    LD --> P3

    KP --> RN

    P1W --> P1D
    P1W --> P1
    P1D --> P1
    P1  --> AS

    P2W --> P2D
    P2W --> P2
    P2D --> P2
    P2  --> AS

    P3W --> P3
    P3  --> AS

    AS  --> RN

    style MO  fill:#dfe6e9,stroke:#636e72,color:#000
    style IM  fill:#dfe6e9,stroke:#636e72,color:#000
    style HD  fill:#74b9ff,stroke:#0984e3,color:#000
    style CT  fill:#a29bfe,stroke:#6c5ce7,color:#000
    style LD  fill:#fd79a8,stroke:#e84393,color:#000
    style KP  fill:#55efc4,stroke:#00b894,color:#000
    style P1W fill:#a29bfe,stroke:#6c5ce7,color:#000
    style P1D fill:#fd79a8,stroke:#e84393,color:#000
    style P1  fill:#55efc4,stroke:#00b894,color:#000
    style P2W fill:#a29bfe,stroke:#6c5ce7,color:#000
    style P2D fill:#fd79a8,stroke:#e84393,color:#000
    style P2  fill:#55efc4,stroke:#00b894,color:#000
    style P3W fill:#a29bfe,stroke:#6c5ce7,color:#000
    style P3  fill:#fd79a8,stroke:#e84393,color:#000
    style AS  fill:#ffeaa7,stroke:#fdcb6e,color:#000
    style RN  fill:#2d3436,stroke:#000,color:#fff
```

**Color key:**
- ⬜ Grey — bootstrap / imports
- 🔵 Blue — header (read-only render)
- 🟣 Purple — CREATE widget cells
- 🔴 Pink — READ `.value` + data cells
- 🟢 Green — panel render cells
- 🟡 Yellow — assembly
- ⬛ Black — final DOM output

---

*© 2026 Mind Over Metadata LLC — Peter Heller. All rights reserved.*
