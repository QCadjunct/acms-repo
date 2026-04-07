"""
ui/aces_monitor.py

ACES Monitor - Six-Tab Marimo Notebook.
Architecture Standard: Mind Over Metadata LLC - Peter Heller

Tabs:
    1. Audit Trail   - sessions + step entries + aggregations
    2. Registry      - skill/task registry analytics
    3. Pipeline      - per-session Mermaid execution graph
    4. Cost          - aggregate KPIs + breakdown by vendor/agent/session
    5. Cost Detail   - row-level drill-down with filters + CSV export
    6. About         - mock documentation, rate card, AgentType registry,
                       future: ACES Task Groups

Marimo rules enforced:
  1. CREATE widget in one cell, READ .value in the next.
  2. Never access .value in the cell that created the widget.
  3. Guard every DuckDB register() with a non-empty check.
  4. Polars 1.x filters use pl.col() - no bare Series predicates.
  5. No pandas - mo.ui.table() accepts Polars DataFrames natively.
     DuckDB returns Polars via .pl(). Empty frames use pl.DataFrame(schema={}).
  6. Accordions: first section open, rest closed per tab.

AgentType registry:
    agent    -> anthropic / claude-sonnet-4-6   LLM call
    subagent -> google    / gemini-2.0-flash    LLM call (delegated)
    team     -> google    / gemini-2.0-flash    LLM call (parallel)
    python   -> ollama    / qwen3:8b            local python execution
    bash     -> ollama    / qwen3:8b            local bash execution - zero LLM cost

Cost accounting standard - Mind Over Metadata LLC:
  Rate card (per token):
    Anthropic  in=$0.000003    out=$0.000015
    Gemini     in=$0.000000375 out=$0.0000015
    OpenAI     in=$0.000005    out=$0.000015
    Ollama     in=$0.000000    out=$0.000000  (bash + python)

Run as notebook:  marimo edit ui/aces_monitor.py
Run as app:       marimo run  ui/aces_monitor.py
"""

import marimo as mo

app = mo.App(width="full", app_title="ACES Monitor - Mind Over Metadata LLC")


# ── Core dependencies ─────────────────────────────────────────────────────────

@app.cell
def _mo():
    import marimo as mo
    return (mo,)


@app.cell
def _imports():
    import duckdb
    import polars as pl
    from ui.data.loader import (
        load_sessions, load_registry,
        sessions_to_df, entries_to_df, skill_records_to_df,
        using_live_db,
    )
    return (duckdb, pl,
            load_sessions, load_registry,
            sessions_to_df, entries_to_df, skill_records_to_df,
            using_live_db)


# ── Header ────────────────────────────────────────────────────────────────────

@app.cell
def _header(mo, using_live_db):
    _src = (
        mo.md("🟢 **Live PostgreSQL**").callout(kind="success")
        if using_live_db()
        else mo.md("🟡 **Mock data** - set `ACES_DATABASE_URL` in `.env` for live").callout(kind="warn")
    )
    header = mo.vstack([
        mo.md("# 🏛️ ACES Monitor"),
        mo.md("**Mind Over Metadata LLC** - Peter Heller &nbsp;|&nbsp; `QCadjunct/acms-langgraph-poc`"),
        _src,
    ])
    return (header,)


# ── Global controls (CREATE only - no .value here) ────────────────────────────

@app.cell
def _controls(mo):
    session_count = mo.ui.slider(5, 50, value=10, step=5, label="Sessions")
    mock_seed     = mo.ui.number(start=1, stop=999, value=42, label="Seed")
    refresh_btn   = mo.ui.button(label="⟳ Refresh", kind="success")
    return session_count, mock_seed, refresh_btn


# ── Data load (READ .value from controls) ─────────────────────────────────────

@app.cell
def _load_data(
    session_count, mock_seed, refresh_btn,
    load_sessions, load_registry,
    sessions_to_df, entries_to_df, skill_records_to_df,
):
    _  = refresh_btn.value
    _n = session_count.value
    _s = mock_seed.value
    sessions   = load_sessions(count=_n, seed=_s)
    registry   = load_registry(seed=_s)
    session_df = sessions_to_df(sessions)
    entry_df   = entries_to_df(sessions)
    skill_df   = skill_records_to_df(registry)
    return sessions, registry, session_df, entry_df, skill_df


# ── KPI bar ───────────────────────────────────────────────────────────────────

@app.cell
def _kpis(mo, session_df, entry_df, pl):
    _tot  = len(session_df)
    _comp = session_df.filter(pl.col("status") == "completed").height
    _fail = session_df.filter(pl.col("is_failed") == True).height  # noqa: E712
    _ret  = (entry_df.filter(pl.col("status") == "retried").height
             if not entry_df.is_empty() else 0)
    _avg  = session_df["total_ms"].mean() or 0.0
    _rate = round((_fail / _tot * 100) if _tot > 0 else 0.0, 1)
    kpis = mo.hstack([
        mo.stat(label="Sessions",   value=str(_tot)),
        mo.stat(label="Completed",  value=str(_comp),    bordered=True),
        mo.stat(label="Failed",     value=str(_fail),    bordered=True),
        mo.stat(label="Retries",    value=str(_ret),     bordered=True),
        mo.stat(label="Avg ms",     value=f"{_avg:.0f}", bordered=True),
        mo.stat(label="Error Rate", value=f"{_rate}%",   bordered=True),
    ], justify="start")
    return (kpis,)


# ══════════════════════════════════════════════════════════════════════════════
# PANEL 1 - Audit Trail Explorer
# ══════════════════════════════════════════════════════════════════════════════

@app.cell
def _p1_widgets(mo):
    """CREATE widgets - no .value access here."""
    p1_status = mo.ui.dropdown(
        options=["all", "completed", "failed"], value="all", label="Status")
    p1_mode = mo.ui.dropdown(
        options=["all", "maas", "cloud", "hybrid"], value="all", label="Mode")
    p1_agents = mo.ui.multiselect(
        options=["agent", "subagent", "team", "python", "bash"],
        value=["agent", "subagent", "team", "python", "bash"],
        label="Agent types")
    return p1_status, p1_mode, p1_agents


@app.cell
def _p1_data(
    p1_status, p1_mode, p1_agents,
    session_df, entry_df, duckdb, pl,
):
    """READ .value - filter and aggregate. All frames are Polars."""
    _sdf = session_df
    if p1_status.value != "all":
        _sdf = _sdf.filter(pl.col("status") == p1_status.value)
    if p1_mode.value != "all":
        _sdf = _sdf.filter(pl.col("operating_mode") == p1_mode.value)

    _edf = entry_df
    _sel = p1_agents.value or []
    if not _edf.is_empty() and _sel:
        _edf = _edf.filter(pl.col("agent_type").is_in(_sel))

    if not entry_df.is_empty():
        _con = duckdb.connect()
        _con.register("e", entry_df.to_arrow())
        _dur = _con.execute(
            "SELECT agent_type, ROUND(AVG(duration_ms),0) AS avg_ms,"
            " COUNT(*) AS cnt FROM e GROUP BY agent_type ORDER BY avg_ms DESC"
        ).pl()
        _sts = _con.execute(
            "SELECT status, COUNT(*) AS cnt FROM e GROUP BY status ORDER BY cnt DESC"
        ).pl()
        _con.close()
    else:
        _dur = pl.DataFrame(schema={"agent_type": pl.Utf8, "avg_ms": pl.Float64, "cnt": pl.Int64})
        _sts = pl.DataFrame(schema={"status": pl.Utf8, "cnt": pl.Int64})

    p1_sess = _sdf
    p1_entr = _edf
    p1_dur  = _dur
    p1_sts  = _sts
    return p1_sess, p1_entr, p1_dur, p1_sts


@app.cell
def _panel1(mo, p1_status, p1_mode, p1_agents,
            p1_sess, p1_entr, p1_dur, p1_sts):
    _filters = mo.hstack([p1_status, p1_mode, p1_agents], justify="start")
    _st = mo.ui.table(p1_sess, label="Sessions",           selection=None, page_size=8)
    _et = (mo.ui.table(p1_entr, label="Step entries",      selection=None, page_size=10)
           if not p1_entr.is_empty() else mo.md("_No step entries_"))
    _dt = (mo.ui.table(p1_dur,  label="Duration by agent", selection=None)
           if not p1_dur.is_empty()  else mo.md("_No data_"))
    _ss = (mo.ui.table(p1_sts,  label="Status counts",     selection=None)
           if not p1_sts.is_empty()  else mo.md("_No data_"))

    panel1 = mo.vstack([
        mo.md("## 📋 Audit Trail Explorer"),
        mo.accordion({
            "Sessions": mo.vstack([_filters, _st]),
            "Step Entries": _et,
            "Aggregations": mo.hstack([_dt, _ss], justify="start"),
        }, multiple=True),
    ])
    return (panel1,)


# ══════════════════════════════════════════════════════════════════════════════
# PANEL 2 - Registry Analytics
# ══════════════════════════════════════════════════════════════════════════════

@app.cell
def _p2_widgets(mo, skill_df):
    """CREATE widgets."""
    _doms = (sorted(skill_df["domain"].unique().to_list())
             if not skill_df.is_empty() else [])
    p2_domain  = mo.ui.multiselect(options=_doms, value=[], label="Domain filter")
    p2_current = mo.ui.checkbox(value=True, label="Current versions only")
    return p2_domain, p2_current


@app.cell
def _p2_data(
    p2_domain, p2_current, skill_df, registry, duckdb, pl,
):
    """READ .value - filter and aggregate. All frames are Polars."""
    _sdf = skill_df
    if not _sdf.is_empty():
        if p2_current.value:
            _sdf = _sdf.filter(pl.col("is_current") == True)  # noqa: E712
        _sel2 = p2_domain.value or []
        if _sel2:
            _sdf = _sdf.filter(pl.col("domain").is_in(_sel2))

    if not skill_df.is_empty():
        _con2 = duckdb.connect()
        _con2.register("s", skill_df.to_arrow())
        _dom = _con2.execute(
            "SELECT domain, COUNT(*) AS total,"
            " SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current"
            " FROM s GROUP BY domain ORDER BY total DESC"
        ).pl()
        _ver = _con2.execute(
            "SELECT fqsn, COUNT(*) AS versions FROM s GROUP BY fqsn ORDER BY versions DESC"
        ).pl()
        _con2.close()
    else:
        _dom = pl.DataFrame(schema={"domain": pl.Utf8, "total": pl.Int64, "current": pl.Int64})
        _ver = pl.DataFrame(schema={"fqsn": pl.Utf8, "versions": pl.Int64})

    _trecs  = registry.get("task_records", []) if registry else []
    p2_task = pl.from_dicts(_trecs) if _trecs else pl.DataFrame()
    p2_skill = _sdf
    p2_dom   = _dom
    p2_ver   = _ver
    return p2_skill, p2_dom, p2_ver, p2_task


@app.cell
def _panel2(mo, p2_domain, p2_current,
            p2_skill, p2_dom, p2_ver, p2_task):
    _filters2 = mo.hstack([p2_domain, p2_current], justify="start")
    _d = (mo.ui.table(p2_dom,   label="Domain summary",  selection=None)
          if not p2_dom.is_empty()   else mo.md("_No data_"))
    _v = (mo.ui.table(p2_ver,   label="Version history", selection=None)
          if not p2_ver.is_empty()   else mo.md("_No data_"))
    _k = (mo.ui.table(p2_skill, label="Skills",          selection=None, page_size=10)
          if not p2_skill.is_empty() else mo.md("_No skills_"))
    _t = (mo.ui.table(p2_task,  label="Task records",    selection=None)
          if not p2_task.is_empty()  else mo.md("_No task records_"))

    panel2 = mo.vstack([
        mo.md("## 🗂️ Registry Analytics"),
        mo.accordion({
            "Domain Summary + Version History": mo.vstack([
                _filters2,
                mo.hstack([_d, _v], justify="start"),
            ]),
            "Skills": _k,
            "Tasks": _t,
        }, multiple=True),
    ])
    return (panel2,)


# ══════════════════════════════════════════════════════════════════════════════
# PANEL 3 - Pipeline Dashboard
# ══════════════════════════════════════════════════════════════════════════════

@app.cell
def _p3_widget(mo, sessions):
    """CREATE session selector."""
    _opts = (
        {
            f"{s['session_id'][:8]} | {s['status'].upper()} | {s['total_duration_ms']:.0f}ms": i
            for i, s in enumerate(sessions)
        }
        if sessions
        else {"No sessions": 0}
    )
    _default = list(_opts.keys())[0]
    p3_select = mo.ui.dropdown(options=_opts, value=_default, label="Select session")
    return (p3_select,)


@app.cell
def _panel3(mo, p3_select, sessions, pl):
    """READ .value - build mermaid diagram + step table as Polars."""
    _idx = p3_select.value if p3_select.value is not None else 0
    _s   = sessions[_idx] if sessions and _idx < len(sessions) else {}

    _STATUS_COLOR = {
        "completed": "#96ceb4",
        "failed":    "#ff6b35",
        "retried":   "#fdcb6e",
        "skipped":   "#b2bec3",
    }

    def _mermaid_diagram(s):
        lines = ["graph TD", "    START([▶ START])"]
        prev  = "START"
        for e in s.get("entries", []):
            _step = e.get("step", 0)
            _eid  = e["entry_id"][:4]
            k     = f"n{_step}_{_eid}"
            at    = e.get("agent_type", "")
            sk    = e.get("fqsn_path", "").split("/")[-1] or "?"
            st    = e.get("status", "")
            ic    = {"agent": "A", "subagent": "S", "team": "T",
                     "python": "P", "bash": "B"}.get(at, "?")
            col   = _STATUS_COLOR.get(st, "#dfe6e9")
            lines += [
                f'    {k}["{ic}:{sk}\\n{st}"]',
                f"    {prev} --> {k}",
                f"    style {k} fill:{col},color:#000",
            ]
            prev = k
            for i, sub in enumerate(e.get("sub_entries", [])[:3]):
                sk2 = f"sub{_step}_{i}"
                ss2 = sub.get("fqsn_path", "").split("/")[-1] or "sub"
                lines += [
                    f'    {sk2}(("{ss2}"))',
                    f"    {k} -.-> {sk2}",
                ]
        lines += ["    END_NODE([⏹ END])", f"    {prev} --> END_NODE"]
        return "\n".join(lines)

    if _s:
        _cards = mo.hstack([
            mo.stat(label="Session",  value=_s["session_id"][:8]),
            mo.stat(label="Status",   value=_s["status"].upper(),               bordered=True),
            mo.stat(label="Duration", value=f"{_s['total_duration_ms']:.0f}ms", bordered=True),
            mo.stat(label="Steps",    value=str(_s["step_count"]),              bordered=True),
            mo.stat(label="Errors",   value=str(_s["error_count"]),             bordered=True),
        ], justify="start")
        _diag = mo.mermaid(_mermaid_diagram(_s))
        _rows = [
            {
                "step":   e.get("step"),
                "type":   e.get("agent_type"),
                "skill":  e.get("fqsn_path", "").split("/")[-1],
                "status": e.get("status"),
                "ms":     f"{e.get('duration_ms', 0):.0f}",
                "retry":  e.get("retry_count", 0),
                "error":  (e.get("error") or "")[:60],
            }
            for e in _s.get("entries", [])
        ]
        _step_df = pl.from_dicts(_rows) if _rows else pl.DataFrame()
        _tbl = (mo.ui.table(_step_df, label="Step detail", selection=None)
                if not _step_df.is_empty() else mo.md("_No steps_"))
        _overview = mo.vstack([p3_select, _cards])
    else:
        _overview = mo.vstack([p3_select, mo.md("_No session selected_")])
        _diag     = mo.md("_Select a session above_")
        _tbl      = mo.md("_No entries_")

    panel3 = mo.vstack([
        mo.md("## 🔭 Pipeline Dashboard"),
        mo.accordion({
            "Session Selector + Cards": _overview,
            "Execution Graph":          _diag,
            "Step Detail":              _tbl,
        }, multiple=True),
    ])
    return (panel3,)


# ══════════════════════════════════════════════════════════════════════════════
# COST ENGINE
# Shared by Panel 4 (aggregate) and Panel 5 (detail).
#
# AgentType.BASH added - zero LLM cost, ollama/qwen3:8b vendor slot.
# Vendor/model derived from agent_type - mock proxy.
# Live WorkspaceEntry will carry cost fields directly.
# Token proxy: duration_ms * factor - deterministic, no randomness.
#
# Rate card - Mind Over Metadata LLC cost accounting standard:
#   agent    -> anthropic / claude-sonnet-4-6   in=$0.000003    out=$0.000015
#   subagent -> google    / gemini-2.0-flash    in=$0.000000375 out=$0.0000015
#   team     -> google    / gemini-2.0-flash    in=$0.000000375 out=$0.0000015
#   python   -> ollama    / qwen3:8b            in=$0.000000    out=$0.000000
#   bash     -> ollama    / qwen3:8b            in=$0.000000    out=$0.000000
# ══════════════════════════════════════════════════════════════════════════════

@app.cell
def _cost_engine(sessions, pl):
    """Build cost_df - one row per entry with vendor, model, tokens, cost_usd."""

    _RATES = {
        "anthropic": {"in": 0.000003,     "out": 0.000015},
        "google":    {"in": 0.000000375,  "out": 0.0000015},
        "openai":    {"in": 0.000005,     "out": 0.000015},
        "ollama":    {"in": 0.0,          "out": 0.0},
    }
    _VENDOR_MAP = {
        "agent":    ("anthropic", "claude-sonnet-4-6"),
        "subagent": ("google",    "gemini-2.0-flash"),
        "team":     ("google",    "gemini-2.0-flash"),
        "python":   ("ollama",    "qwen3:8b"),
        "bash":     ("ollama",    "qwen3:8b"),   # zero cost - no LLM call
    }

    rows = []
    for _sess in sessions:
        _sid = str(_sess.get("session_id", ""))[:8]
        for _e in _sess.get("entries", []):
            _at     = _e.get("agent_type", "python")
            _dur    = float(_e.get("duration_ms", 0.0))
            _vendor, _model = _VENDOR_MAP.get(_at, ("ollama", "qwen3:8b"))
            _rate   = _RATES[_vendor]
            _in     = max(100, int(_dur * 0.4))
            _out    = max(20,  int(_dur * 0.15))
            _cost   = round(_in * _rate["in"] + _out * _rate["out"], 8)
            rows.append({
                "session_id":  _sid,
                "step":        int(_e.get("step", 0)),
                "agent_type":  _at,
                "skill":       _e.get("fqsn_path", "").split("/")[-1],
                "skill_fqsn":  _e.get("fqsn_path", ""),
                "status":      _e.get("status", ""),
                "vendor":      _vendor,
                "model":       _model,
                "duration_ms": _dur,
                "in_tokens":   _in,
                "out_tokens":  _out,
                "cost_usd":    _cost,
            })

    cost_df = pl.from_dicts(rows) if rows else pl.DataFrame()
    return (cost_df,)


# ══════════════════════════════════════════════════════════════════════════════
# PANEL 4 - Cost Summary (aggregate)
# ══════════════════════════════════════════════════════════════════════════════

@app.cell
def _panel4(mo, cost_df, duckdb, pl):
    """Aggregate cost KPIs + breakdowns by vendor, agent type, session."""

    if cost_df.is_empty():
        panel4 = mo.vstack([
            mo.md("## 💰 Cost Summary"),
            mo.md("_No cost data available_"),
        ])
    else:
        _con4 = duckdb.connect()
        _con4.register("c", cost_df.to_arrow())

        _row = _con4.execute("""
            SELECT
                COUNT(DISTINCT session_id)     AS sessions,
                SUM(cost_usd)                  AS total_cost,
                AVG(cost_usd)                  AS avg_per_entry,
                SUM(in_tokens)                 AS total_in,
                SUM(out_tokens)                AS total_out,
                SUM(in_tokens + out_tokens)    AS total_tokens
            FROM c
        """).fetchone()

        _n_sess   = int(_row[0])
        _tot_cost = float(_row[1] or 0.0)
        _avg_ent  = float(_row[2] or 0.0)
        _tot_in   = int(_row[3] or 0)
        _tot_out  = int(_row[4] or 0)
        _tot_tok  = int(_row[5] or 0)
        _avg_sess = round(_tot_cost / _n_sess, 8) if _n_sess > 0 else 0.0

        _vendor_df = _con4.execute("""
            SELECT vendor, model,
                COUNT(*)                AS calls,
                SUM(in_tokens)          AS in_tokens,
                SUM(out_tokens)         AS out_tokens,
                ROUND(SUM(cost_usd),8)  AS total_cost_usd,
                ROUND(AVG(cost_usd),8)  AS avg_cost_usd
            FROM c GROUP BY vendor, model ORDER BY total_cost_usd DESC
        """).pl()

        _agent_df = _con4.execute("""
            SELECT agent_type,
                COUNT(*)                   AS calls,
                ROUND(AVG(duration_ms),0)  AS avg_ms,
                SUM(in_tokens)             AS in_tokens,
                SUM(out_tokens)            AS out_tokens,
                ROUND(SUM(cost_usd),8)     AS total_cost_usd,
                ROUND(AVG(cost_usd),8)     AS avg_cost_usd
            FROM c GROUP BY agent_type ORDER BY total_cost_usd DESC
        """).pl()

        _sess_df = _con4.execute("""
            SELECT session_id,
                COUNT(*)                AS steps,
                SUM(in_tokens)          AS in_tokens,
                SUM(out_tokens)         AS out_tokens,
                ROUND(SUM(cost_usd),8)  AS total_cost_usd
            FROM c GROUP BY session_id ORDER BY total_cost_usd DESC
        """).pl()

        _con4.close()
        _toon_saved = round(_tot_out * 0.19)

        _kpi_row = mo.hstack([
            mo.stat(label="Total cost",    value=f"${_tot_cost:.6f}"),
            mo.stat(label="Avg / session", value=f"${_avg_sess:.6f}", bordered=True),
            mo.stat(label="Avg / entry",   value=f"${_avg_ent:.6f}",  bordered=True),
            mo.stat(label="In tokens",     value=f"{_tot_in:,}",      bordered=True),
            mo.stat(label="Out tokens",    value=f"{_tot_out:,}",     bordered=True),
            mo.stat(label="Total tokens",  value=f"{_tot_tok:,}",     bordered=True),
        ], justify="start")

        _toon_note = mo.md(
            f"> **TOON efficiency** - Token-Optimized Object Notation delivers ~19% token "
            f"reduction vs YAML on wire format. "
            f"Estimated tokens saved this load: **{_toon_saved:,}** out tokens.  \n"
            f"> Rate card: "
            f"Anthropic in=\\$0.000003/out=\\$0.000015 · "
            f"Gemini in=\\$0.000000375/out=\\$0.0000015 · "
            f"Ollama=\\$0.000000 (python + bash)"
        ).callout(kind="info")

        panel4 = mo.vstack([
            mo.md("## 💰 Cost Summary"),
            mo.accordion({
                "KPIs": _kpi_row,
                "By Vendor / Model": (
                    mo.ui.table(_vendor_df, label="Cost by vendor / model", selection=None)
                    if not _vendor_df.is_empty() else mo.md("_No data_")
                ),
                "By Agent Type": (
                    mo.ui.table(_agent_df, label="Cost by agent type", selection=None)
                    if not _agent_df.is_empty() else mo.md("_No data_")
                ),
                "By Session": (
                    mo.ui.table(_sess_df, label="Cost by session", selection=None, page_size=10)
                    if not _sess_df.is_empty() else mo.md("_No data_")
                ),
                "TOON Efficiency": _toon_note,
            }, multiple=True),
        ])

    return (panel4,)


# ══════════════════════════════════════════════════════════════════════════════
# PANEL 5 - Cost Detail (row-level drill-down)
# ══════════════════════════════════════════════════════════════════════════════

@app.cell
def _p5_widgets(mo, cost_df):
    """CREATE filter widgets - no .value access here."""
    _vendors  = sorted(cost_df["vendor"].unique().to_list())     if not cost_df.is_empty() else []
    _agents   = sorted(cost_df["agent_type"].unique().to_list()) if not cost_df.is_empty() else []
    _sessions = sorted(cost_df["session_id"].unique().to_list()) if not cost_df.is_empty() else []

    p5_vendor  = mo.ui.multiselect(options=_vendors,  value=_vendors, label="Vendor")
    p5_agent   = mo.ui.multiselect(options=_agents,   value=_agents,  label="Agent type")
    p5_session = mo.ui.multiselect(options=_sessions, value=[],       label="Session (blank = all)")
    p5_status  = mo.ui.dropdown(
        options=["all", "completed", "failed", "retried"],
        value="all", label="Status")
    return p5_vendor, p5_agent, p5_session, p5_status


@app.cell
def _p5_data(p5_vendor, p5_agent, p5_session, p5_status, cost_df, pl):
    """READ .value - apply filters, compute filtered KPIs. Pure Polars."""
    _df = cost_df
    if not _df.is_empty():
        _v = p5_vendor.value or []
        if _v:
            _df = _df.filter(pl.col("vendor").is_in(_v))
        _a = p5_agent.value or []
        if _a:
            _df = _df.filter(pl.col("agent_type").is_in(_a))
        _ss = p5_session.value or []
        if _ss:
            _df = _df.filter(pl.col("session_id").is_in(_ss))
        if p5_status.value != "all":
            _df = _df.filter(pl.col("status") == p5_status.value)

    p5_detail    = _df
    p5_filt_cost = float(_df["cost_usd"].sum())  if not _df.is_empty() else 0.0
    p5_filt_rows = len(_df)
    p5_filt_in   = int(_df["in_tokens"].sum())   if not _df.is_empty() else 0
    p5_filt_out  = int(_df["out_tokens"].sum())  if not _df.is_empty() else 0
    return p5_detail, p5_filt_cost, p5_filt_rows, p5_filt_in, p5_filt_out


@app.cell
def _panel5(mo, p5_vendor, p5_agent, p5_session, p5_status,
            p5_detail, p5_filt_cost, p5_filt_rows, p5_filt_in, p5_filt_out):
    """Render Cost Detail panel."""
    _filt_kpis = mo.hstack([
        mo.stat(label="Filtered rows", value=str(p5_filt_rows)),
        mo.stat(label="Filtered cost", value=f"${p5_filt_cost:.6f}", bordered=True),
        mo.stat(label="In tokens",     value=f"{p5_filt_in:,}",      bordered=True),
        mo.stat(label="Out tokens",    value=f"{p5_filt_out:,}",     bordered=True),
    ], justify="start")

    _filters5   = mo.hstack([p5_vendor, p5_agent, p5_session, p5_status], justify="start")
    _detail_tbl = (
        mo.ui.table(p5_detail, label="Cost detail", selection=None, page_size=15)
        if not p5_detail.is_empty() else mo.md("_No rows match current filters_")
    )
    _export_note = mo.md(
        "> **Export** - use the Download button in the table toolbar to save filtered rows as CSV.  \n"
        "> Columns: `session_id | step | agent_type | skill | skill_fqsn | status |"
        " vendor | model | duration_ms | in_tokens | out_tokens | cost_usd`  \n"
        "> When live data is available, token and cost fields will carry actual "
        "values from `WorkspaceEntry` - no proxy needed."
    ).callout(kind="neutral")

    panel5 = mo.vstack([
        mo.md("## 🔍 Cost Detail"),
        mo.accordion({
            "Filters + KPIs": mo.vstack([_filters5, _filt_kpis]),
            "Detail Table + Export": mo.vstack([_detail_tbl, _export_note]),
        }, multiple=True),
    ])
    return (panel5,)


# ══════════════════════════════════════════════════════════════════════════════
# PANEL 6 - About
# No accordion - flat documentation page.
# ══════════════════════════════════════════════════════════════════════════════

@app.cell
def _panel6(mo):
    """Static documentation - no reactive dependencies."""

    panel6 = mo.vstack([
        mo.md("## ℹ️ About ACES Monitor"),

        mo.md("### Mock Data").callout(kind="warn"),
        mo.md("""
This monitor runs on **deterministic mock data** generated by `ui/data/mock.py`.

The same `seed` value always produces the same sessions, entries, and registry records.
Change the **Seed** control in the header to explore different data shapes.
Set `ACES_DATABASE_URL` in your `.env` to switch to live PostgreSQL automatically -
the loader is transparent; the UI receives identical dict structures either way.

**Four mock scenarios - exercising every UI code path:**

| # | Scenario | Description |
|---|----------|-------------|
| 1 | Happy path | All four steps complete without error |
| 2 | Retry then success | Step 1 fails, retries, recovers |
| 3 | Team partial fail | One team member fails - non-blocking |
| 4 | Hard failure | Persist fails, `FAIL_TASK`, session failed |
        """),

        mo.md("### AgentType Registry"),
        mo.md("""
| AgentType | Icon | Vendor | Model | LLM call | Cost |
|-----------|------|--------|-------|----------|------|
| `agent` | A | Anthropic | claude-sonnet-4-6 | ✅ | $0.000003/in · $0.000015/out |
| `subagent` | S | Google | gemini-2.0-flash | ✅ | $0.000000375/in · $0.0000015/out |
| `team` | T | Google | gemini-2.0-flash | ✅ | $0.000000375/in · $0.0000015/out |
| `python` | P | Ollama | qwen3:8b | ❌ | $0.000000 - local execution |
| `bash` | B | Ollama | qwen3:8b | ❌ | $0.000000 - local execution |

`python` and `bash` execute locally - zero LLM cost. Token fields in live data
will be `0` for these agent types. The cost engine correctly maps both to the
Ollama rate card (`$0.000000`).
        """),

        mo.md("### Cost Accounting Standard"),
        mo.md("""
Every bash utility in the ACES pipeline includes **cost accounting as Step N** -
a standard component, not an afterthought.

**Rate card (per token):**

| Vendor | In ($/token) | Out ($/token) |
|--------|-------------|--------------|
| Anthropic | $0.000003 | $0.000015 |
| Google (Gemini) | $0.000000375 | $0.0000015 |
| OpenAI | $0.000005 | $0.000015 |
| Ollama (local) | $0.000000 | $0.000000 |

**Mock proxy:** Until `WorkspaceEntry` carries live `in_tokens`, `out_tokens`,
and `cost_usd` fields, the cost engine derives tokens from `duration_ms` using a
deterministic factor (`in = duration_ms × 0.4`, `out = duration_ms × 0.15`).
This proxy is replaced with zero code change when live fields arrive.

**TOON efficiency:** Token-Optimized Object Notation delivers ~19% token reduction
vs YAML on wire format - validated: 392 vs 482 out tokens on `ACES_extract_wisdom`.
        """),

        mo.md("### Future - ACES Task Groups"),
        mo.md("""
**Status: not yet implemented - specced here for the roadmap.**

A **Task Group** is a named collection of related sessions sharing a common
business objective. Task Groups sit above sessions in the hierarchy:

```
TaskGroup
  └── Session (1..N)
        └── WorkspaceEntry / step (1..N)
```

**Planned additions to this monitor when Task Groups land:**

- **Audit Trail** - group sessions by `task_group_id`; show group-level status rollup
- **Registry** - `TaskGroup` records alongside `Task` and `Skill` records
- **Pipeline** - group selector above session selector; show group execution timeline
- **Cost** - cost roll-up by task group; group vs session cost comparison
- **Cost Detail** - `task_group_id` filter column added to drill-down table

**Data model sketch:**

| Field | Type | Notes |
|-------|------|-------|
| `task_group_id` | UUID | Primary key |
| `task_group_name` | str | Human-readable label |
| `task_fqsn` | str | FK → task_record |
| `session_ids` | list[UUID] | Member sessions |
| `status` | str | `pending / running / completed / failed` |
| `created_at` | datetime | Group creation timestamp |
| `completed_at` | datetime | Last session completion |

ARUM (Agentic Resource Utilization Monitor) integration will surface
task group cost and throughput metrics as a sidecar to this monitor.
        """),
    ])
    return (panel6,)


# ══════════════════════════════════════════════════════════════════════════════
# PANEL 7 - D⁴ MDLC Cost Intelligence
# Reads cost_audit.log directly - ADR-009 16-field pipe-delimited format.
# Backward compatible with legacy format entries.
#
# Architecture Standard: Mind Over Metadata LLC - Peter Heller
# ADR-009: D⁴ MDLC Governance for Agentic AI Systems
#
# Marimo rules:
#   1. CREATE widgets in _p7_widgets - no .value access
#   2. READ .value in _p7_data
#   3. Render in _panel7
#   4. All frames are Polars - no pandas
# ══════════════════════════════════════════════════════════════════════════════

@app.cell
def _d4_load_log(pl):
    """Load cost_audit.log — ADR-009 + legacy. Inner function avoids top-level return."""
    import re as _re
    from pathlib import Path as _Path

    # Try WSL2 UNC path (Windows Marimo -> WSL2 log), fallback to native
    _wsl_log = _Path("\\\\wsl$\\Ubuntu\\home\\pheller\\.config\\fabric\\cost_audit.log")
    _native  = _Path.home() / ".config" / "fabric" / "cost_audit.log"
    _log     = _wsl_log if _wsl_log.exists() else _native

    _TIERS = {
        "principal_system_architect.system.md": "tier_0_elicitation",
        "requirements_identity.system.md":      "tier_0_elicitation",
        "requirements_mission.system.md":       "tier_0_elicitation",
        "requirements_authorities.system.md":   "tier_0_elicitation",
        "requirements_lifecycle.system.md":     "tier_0_elicitation",
        "requirements_cost_model.system.md":    "tier_0_elicitation",
        "requirements_data.system.md":          "tier_0_elicitation",
        "skill.system.md":                      "tier_1_source",
        "transformer.yaml.system.md":           "tier_1_source",
        "transformer.toon.system.md":           "tier_1_source",
        "skill.system.yaml":                    "tier_2_derived",
        "skill.system.toon":                    "tier_2_derived",
        "session.total":                        "tier_4_session",
    }

    def _tier(a):
        if a in _TIERS: return _TIERS[a]
        for p in ("fabric_stitch.", "langgraph.", "hook."): 
            if a.startswith(p): return "tier_3_execution"
        return "tier_unknown"

    def _adr(parts):
        if len(parts) < 6: return False
        return bool(_re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            parts[2].strip(), _re.I))

    def _i(v):
        try: return int(v.strip())
        except: return 0

    def _f(v):
        try: return float(v.strip())
        except: return 0.0

    def _run():
        if not _log.exists():
            return pl.DataFrame(), str(_log), 0, f"No log at {_log}"
        rows, skip = [], 0
        try:
            with open(_log) as fh:
                for ln in fh:
                    ln = ln.strip()
                    if not ln or ln.startswith("#"): continue
                    pts = ln.split(" | ")
                    if len(pts) < 2: skip += 1; continue
                    try:
                        if _adr(pts):
                            while len(pts) < 16: pts.append("")
                            rows.append({
                                "timestamp":    pts[0].strip("[]"),
                                "component":    pts[1].strip(),
                                "run_id":       pts[2].strip(),
                                "skill":        pts[3].strip(),
                                "artifact":     pts[4].strip(),
                                "vendor":       pts[5].strip(),
                                "model":        pts[6].strip(),
                                "tokens_in":    _i(pts[7]),
                                "tokens_out":   _i(pts[8]),
                                "cost_in":      _f(pts[9]),
                                "cost_out":     _f(pts[10]),
                                "cost_total":   _f(pts[11]),
                                "elapsed_ms":   _i(pts[12]),
                                "env":          pts[13].strip(),
                                "upstream_id":  pts[14].strip() if len(pts) > 14 else "",
                                "notes":        pts[15].strip() if len(pts) > 15 else "",
                                "artifact_tier": _tier(pts[4].strip()),
                            })
                        else:
                            raw = " | ".join(pts)
                            def ex(pat, d=""):
                                m = _re.search(pat, raw)
                                return m.group(1) if m else d
                            rows.append({
                                "timestamp":    pts[0].strip("[]"),
                                "component":    pts[1].strip() if len(pts) > 1 else "unknown",
                                "run_id":       "legacy",
                                "skill":        ex(r"skill=([^\s|]+)"),
                                "artifact":     "session.total",
                                "vendor":       ex(r"vendor=([^\s|]+)"),
                                "model":        ex(r"model=([^\s|]+)"),
                                "tokens_in":    _i(ex(r"tokens_in=(\d+)", "0")),
                                "tokens_out":   _i(ex(r"tokens_out=(\d+)", "0")),
                                "cost_in":      0.0,
                                "cost_out":     0.0,
                                "cost_total":   _f(ex(r"cost=\$?([0-9.]+)", "0.0")),
                                "elapsed_ms":   _i(ex(r"elapsed=(\d+)ms", "0")),
                                "env":          ex(r"env=([^\s|]+)", "dev"),
                                "upstream_id":  "",
                                "notes":        "legacy_format",
                                "artifact_tier": "tier_4_session",
                            })
                    except Exception:
                        skip += 1
        except Exception as e:
            return pl.DataFrame(), str(_log), 0, str(e)
        if rows:
            df = pl.DataFrame(rows).with_columns([
                pl.col("tokens_in").cast(pl.Int64),
                pl.col("tokens_out").cast(pl.Int64),
                pl.col("cost_in").cast(pl.Float64),
                pl.col("cost_out").cast(pl.Float64),
                pl.col("cost_total").cast(pl.Float64),
                pl.col("elapsed_ms").cast(pl.Int64),
            ])
        else:
            df = pl.DataFrame()
        return df, str(_log), len(rows), (f"Skipped {skip} lines" if skip else "")

    d4_df, d4_log_path, d4_count, d4_error = _run()
    return d4_df, d4_log_path, d4_count, d4_error


@app.cell
def _p7_widgets(mo, d4_df, pl):
    """CREATE widgets - no .value access."""
    _components = (sorted(d4_df["component"].unique().to_list())
                   if not d4_df.is_empty() else [])
    _envs       = (sorted(d4_df["env"].unique().to_list())
                   if not d4_df.is_empty() else ["dev"])
    _skills     = (sorted(d4_df["skill"].unique().to_list())
                   if not d4_df.is_empty() else [])
    _tiers      = (sorted(d4_df["artifact_tier"].unique().to_list())
                   if not d4_df.is_empty() else [])

    p7_component = mo.ui.multiselect(
        options=_components, value=_components, label="Component")
    p7_env       = mo.ui.multiselect(
        options=_envs, value=_envs, label="Environment")
    p7_skill     = mo.ui.multiselect(
        options=_skills, value=[], label="Skill (blank = all)")
    p7_tier      = mo.ui.multiselect(
        options=_tiers, value=[], label="Artifact tier (blank = all)")
    p7_refresh   = mo.ui.button(label="⟳ Refresh log", kind="success")

    return p7_component, p7_env, p7_skill, p7_tier, p7_refresh


@app.cell
def _p7_data(
    p7_component, p7_env, p7_skill, p7_tier, p7_refresh,
    d4_df, pl, duckdb,
):
    """READ .value - filter and aggregate. All frames are Polars."""
    _ = p7_refresh.value   # trigger reactivity on refresh

    df = d4_df
    if not df.is_empty():
        _c = p7_component.value or []
        if _c:
            df = df.filter(pl.col("component").is_in(_c))
        _e = p7_env.value or []
        if _e:
            df = df.filter(pl.col("env").is_in(_e))
        _s = p7_skill.value or []
        if _s:
            df = df.filter(pl.col("skill").is_in(_s))
        _t = p7_tier.value or []
        if _t:
            df = df.filter(pl.col("artifact_tier").is_in(_t))

    # Aggregate by artifact tier
    if not df.is_empty():
        _con = duckdb.connect()
        _con.register("d", df.to_arrow())

        p7_by_tier = _con.execute("""
            SELECT artifact_tier,
                COUNT(*) AS entries,
                SUM(tokens_in)  AS tokens_in,
                SUM(tokens_out) AS tokens_out,
                ROUND(SUM(cost_total), 8) AS cost_total,
                SUM(elapsed_ms) AS elapsed_ms
            FROM d GROUP BY artifact_tier ORDER BY artifact_tier
        """).pl()

        p7_by_artifact = _con.execute("""
            SELECT artifact, artifact_tier,
                COUNT(*) AS entries,
                SUM(tokens_in)  AS tokens_in,
                SUM(tokens_out) AS tokens_out,
                ROUND(SUM(cost_total), 8) AS cost_total,
                SUM(elapsed_ms) AS elapsed_ms_total
            FROM d GROUP BY artifact, artifact_tier ORDER BY artifact_tier, artifact
        """).pl()

        p7_by_vendor = _con.execute("""
            SELECT vendor, model,
                COUNT(*) AS entries,
                SUM(tokens_in)  AS tokens_in,
                SUM(tokens_out) AS tokens_out,
                ROUND(SUM(cost_total), 8) AS cost_total
            FROM d GROUP BY vendor, model ORDER BY cost_total DESC
        """).pl()

        p7_by_skill = _con.execute("""
            SELECT skill,
                COUNT(DISTINCT run_id) AS runs,
                COUNT(*) AS entries,
                SUM(tokens_in)  AS tokens_in,
                SUM(tokens_out) AS tokens_out,
                ROUND(SUM(cost_total), 8) AS cost_total
            FROM d GROUP BY skill ORDER BY cost_total DESC
        """).pl()

        # Bloat detection - skill.system.md entries
        p7_bloat = _con.execute("""
            SELECT skill,
                MAX(tokens_in) AS max_source_tokens,
                AVG(tokens_in) AS avg_source_tokens,
                COUNT(DISTINCT run_id) AS sync_runs,
                CASE
                    WHEN MAX(tokens_in) > 800 THEN '⚠ REVIEW'
                    WHEN MAX(tokens_in) > 500 THEN '🔶 WATCH'
                    ELSE '✅ OK'
                END AS status,
                MAX(tokens_in) * 3 AS estimated_downstream_tokens
            FROM d WHERE artifact = 'skill.system.md'
            GROUP BY skill ORDER BY max_source_tokens DESC
        """).pl()

        # TOON vs YAML comparison
        _yaml = _con.execute("""
            SELECT skill,
                AVG(tokens_out) AS yaml_tokens_out,
                AVG(cost_total) AS yaml_cost
            FROM d WHERE artifact = 'skill.system.yaml'
            GROUP BY skill
        """).pl()
        _toon = _con.execute("""
            SELECT skill,
                AVG(tokens_out) AS toon_tokens_out,
                AVG(cost_total) AS toon_cost
            FROM d WHERE artifact = 'skill.system.toon'
            GROUP BY skill
        """).pl()

        if not _yaml.is_empty() and not _toon.is_empty():
            p7_compare = _yaml.join(_toon, on="skill", how="inner").with_columns([
                ((pl.col("yaml_tokens_out") - pl.col("toon_tokens_out"))
                 / pl.col("yaml_tokens_out") * 100).round(1).alias("reduction_pct"),
                (pl.col("yaml_tokens_out") - pl.col("toon_tokens_out"))
                .alias("tokens_saved"),
            ])
        else:
            p7_compare = pl.DataFrame()

        _con.close()

        # Summary KPIs
        p7_total_cost   = float(df["cost_total"].sum())
        p7_total_tokens = int(df["tokens_in"].sum()) + int(df["tokens_out"].sum())
        p7_runs         = int(df["run_id"].n_unique())
        p7_skills       = int(df["skill"].n_unique())

    else:
        p7_by_tier      = pl.DataFrame()
        p7_by_artifact  = pl.DataFrame()
        p7_by_vendor    = pl.DataFrame()
        p7_by_skill     = pl.DataFrame()
        p7_bloat        = pl.DataFrame()
        p7_compare      = pl.DataFrame()
        p7_total_cost   = 0.0
        p7_total_tokens = 0
        p7_runs         = 0
        p7_skills       = 0

    return (df, p7_by_tier, p7_by_artifact, p7_by_vendor, p7_by_skill,
            p7_bloat, p7_compare, p7_total_cost, p7_total_tokens, p7_runs, p7_skills)


@app.cell
def _panel7(
    mo, pl,
    p7_component, p7_env, p7_skill, p7_tier, p7_refresh,
    d4_df, d4_log_path, d4_count, d4_error,
    df, p7_by_tier, p7_by_artifact, p7_by_vendor, p7_by_skill,
    p7_bloat, p7_compare,
    p7_total_cost, p7_total_tokens, p7_runs, p7_skills,
):
    """Render D⁴ MDLC Cost Intelligence panel."""

    # ── Log status banner ──────────────────────────────────────────────────────
    if d4_df.is_empty():
        _log_status = mo.md(
            f"⚠ **{d4_error}**\n\n"
            f"Log path: `{d4_log_path}`\n\n"
            "Run `sync_skill.sh --source path/to/system.md --generate all --env dev` "
            "to populate the log."
        ).callout(kind="warn")
    else:
        _log_status = mo.md(
            f"🟢 **{d4_count} entries** loaded from `{d4_log_path}`"
            + (f"  \n⚠ {d4_error}" if d4_error else "")
        ).callout(kind="success")

    # ── KPI bar ────────────────────────────────────────────────────────────────
    _kpis = mo.hstack([
        mo.stat(label="Total cost",   value=f"${p7_total_cost:.6f}"),
        mo.stat(label="Total tokens", value=f"{p7_total_tokens:,}",  bordered=True),
        mo.stat(label="Run IDs",      value=str(p7_runs),            bordered=True),
        mo.stat(label="Skills",       value=str(p7_skills),          bordered=True),
        mo.stat(label="Log entries",  value=str(d4_count),           bordered=True),
    ], justify="start")

    # ── Filters ────────────────────────────────────────────────────────────────
    _filters = mo.hstack(
        [p7_component, p7_env, p7_skill, p7_tier, p7_refresh],
        justify="start"
    )

    # ── Tables ─────────────────────────────────────────────────────────────────
    def _tbl(df, label, page_size=10):
        if df is not None and not df.is_empty():
            return mo.ui.table(df, label=label, selection=None, page_size=page_size)
        return mo.md(f"_No data for {label}_")

    # ── TOON note ──────────────────────────────────────────────────────────────
    _avg_reduction = (
        float(p7_compare["reduction_pct"].mean())
        if not p7_compare.is_empty() else None
    )
    if _avg_reduction is not None:
        _toon_status = "✅" if _avg_reduction >= 15 else "⚠"
        _toon_note = mo.md(
            f"> **TOON efficiency** - Average reduction: **{_avg_reduction:.1f}%** vs YAML {_toon_status}  \n"
            f"> Target: ≥15%. Source: `skill.system.toon` vs `skill.system.yaml` output tokens.  \n"
            f"> ADR-009: transformer.toon.system.md is the prompt that produces system.toon - "
            f"if it's verbose, it inflates every downstream consumer's input cost."
        ).callout(kind="success" if _avg_reduction >= 15 else "warn")
    else:
        _toon_note = mo.md(
            "> **TOON efficiency** - No system.yaml / system.toon pairs in log yet.  \n"
            "> Run `sync_skill.sh --generate all` to populate."
        ).callout(kind="neutral")

    # ── Boris Cherney bloat note ───────────────────────────────────────────────
    _bloat_note = mo.md("""
> **Boris Cherney Principle** - A bloated `skill.system.md` inflates every downstream
> consumer's input token count. The transformer prompts (`transformer.yaml.system.md`,
> `transformer.toon.system.md`) are added to `skill.system.md` tokens to form the
> combined input for each generation step.
>
> **Threshold**: > 800 tokens → review recommended · > 500 tokens → watch
>
> **RUN_ID chain**: Each sync generates a `RUN_ID`. Downstream executions carry
> `UPSTREAM_ID` pointing to the sync that produced their artifacts - enabling
> full cost-of-ownership queries across the D⁴ MDLC chain.
    """).callout(kind="info")

    # ── D4 MDLC chain note ────────────────────────────────────────────────────
    _chain_note = mo.md("""
**D⁴ MDLC Cost Chain** - tier_0 → tier_1 → tier_2 → tier_3 → tier_4

| Tier | Artifacts | Role |
|------|-----------|------|
| tier_0_elicitation | `requirements_*.system.md` | PrincipalSystemArchitect specialists |
| tier_1_source | `skill.system.md`, `transformer.*.system.md` | Source and prompt inputs |
| tier_2_derived | `skill.system.yaml`, `skill.system.toon` | Generated artifacts |
| tier_3_execution | `fabric_stitch.step_N`, `langgraph.node_*`, `hook.*` | Runtime execution |
| tier_4_session | `session.total` | Aggregated session summary |
    """)

    panel7 = mo.vstack([
        mo.md("## 🧬 D⁴ MDLC Cost Intelligence"),
        mo.md("*ADR-009 - D⁴ MDLC Governance for Agentic AI Systems - Mind Over Metadata LLC*"),
        _log_status,
        mo.accordion({
            "KPIs + Filters": mo.vstack([_kpis, _filters]),
            "Cost by Artifact Tier (D⁴ MDLC Chain)": mo.vstack([
                _chain_note,
                _tbl(p7_by_tier, "Cost by tier"),
            ]),
            "Cost by Artifact (Three-File Standard)": _tbl(p7_by_artifact, "Cost by artifact"),
            "Cost by Vendor / Model": _tbl(p7_by_vendor, "Cost by vendor"),
            "Cost by Skill (FQSN)": _tbl(p7_by_skill, "Cost by skill"),
            "Bloat Detection - skill.system.md": mo.vstack([
                _bloat_note,
                _tbl(p7_bloat, "Bloat detection"),
            ]),
            "TOON Efficiency - yaml vs toon": mo.vstack([
                _toon_note,
                _tbl(p7_compare, "TOON comparison"),
            ]),
        }, multiple=True),
    ])
    return (panel7,)


# ── Final assembly ────────────────────────────────────────────────────────────

@app.cell
def _assemble(mo, panel1, panel2, panel3, panel4, panel5, panel6, panel7):
    tabs = mo.ui.tabs({
        "📋 Audit Trail":       panel1,
        "🗂️ Registry":         panel2,
        "🔭 Pipeline":         panel3,
        "💰 Cost":              panel4,
        "🔍 Cost Detail":       panel5,
        "🧬 D⁴ MDLC":          panel7,
        "ℹ️ About":             panel6,
    })
    return (tabs,)


@app.cell
def _render(mo, header, kpis, session_count, mock_seed, refresh_btn, tabs):
    mo.vstack([
        header,
        mo.hstack([session_count, mock_seed, refresh_btn], justify="start"),
        kpis,
        tabs,
    ])


if __name__ == "__main__":
    app.run()

