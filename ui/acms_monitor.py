"""
ui/acms_monitor.py

ACMS Monitor — Three-Panel Marimo Notebook.
Architecture Standard: Mind Over Metadata LLC — Peter Heller

Marimo rules enforced:
  1. CREATE widget in one cell, READ .value in the next.
  2. Never access .value in the cell that created the widget.
  3. Guard every DuckDB register() with a non-empty check.
  4. Polars 1.x filters use pl.col() — no bare Series predicates.

Run as notebook:  marimo edit ui/acms_monitor.py
Run as app:       marimo run  ui/acms_monitor.py
"""

import marimo as mo

app = mo.App(width="full", app_title="ACMS Monitor — Mind Over Metadata LLC")


# ── Core dependencies ─────────────────────────────────────────────────────────

@app.cell
def _mo():
    import marimo as mo
    return (mo,)


@app.cell
def _imports():
    import json
    import duckdb
    import polars as pl
    import pandas as pd
    from ui.data.loader import (
        load_sessions, load_registry,
        sessions_to_df, entries_to_df, skill_records_to_df,
        using_live_db,
    )
    return (
        json, duckdb, pl, pd,
        load_sessions, load_registry,
        sessions_to_df, entries_to_df, skill_records_to_df,
        using_live_db,
    )


# ── Header ────────────────────────────────────────────────────────────────────

@app.cell
def _header(mo, using_live_db):
    _src = (
        mo.md("🟢 **Live PostgreSQL**").callout(kind="success")
        if using_live_db()
        else mo.md("🟡 **Mock data** — set `ACMS_DATABASE_URL` in `.env` for live").callout(kind="warn")
    )
    header = mo.vstack([
        mo.md("# 🏛️ ACMS Monitor"),
        mo.md("**Mind Over Metadata LLC** — Peter Heller &nbsp;|&nbsp; `QCadjunct/acms-langgraph-poc`"),
        _src,
    ])
    return (header,)


# ── Global controls (CREATE only — no .value here) ────────────────────────────

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
    _  = refresh_btn.value          # reactive trigger
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
        mo.stat(label="Completed",  value=str(_comp),      bordered=True),
        mo.stat(label="Failed",     value=str(_fail),      bordered=True),
        mo.stat(label="Retries",    value=str(_ret),       bordered=True),
        mo.stat(label="Avg ms",     value=f"{_avg:.0f}",   bordered=True),
        mo.stat(label="Error Rate", value=f"{_rate}%",     bordered=True),
    ], justify="start")
    return (kpis,)


# ══════════════════════════════════════════════════════════════════════════════
# PANEL 1 — Audit Trail Explorer
# ══════════════════════════════════════════════════════════════════════════════

@app.cell
def _p1_widgets(mo):
    """CREATE widgets — no .value access here."""
    p1_status = mo.ui.dropdown(
        options=["all", "completed", "failed"], value="all", label="Status")
    p1_mode = mo.ui.dropdown(
        options=["all", "maas", "cloud", "hybrid"], value="all", label="Mode")
    p1_agents = mo.ui.multiselect(
        options=["agent", "subagent", "team", "python"],
        value=["agent", "subagent", "team", "python"],
        label="Agent types",
    )
    return p1_status, p1_mode, p1_agents


@app.cell
def _p1_data(
    p1_status, p1_mode, p1_agents,
    session_df, entry_df,
    duckdb, pd, pl,
):
    """READ .value — filter and aggregate."""
    _sdf = session_df
    if p1_status.value != "all":
        _sdf = _sdf.filter(pl.col("status") == p1_status.value)
    if p1_mode.value != "all":
        _sdf = _sdf.filter(pl.col("operating_mode") == p1_mode.value)

    _edf = entry_df
    _sel = p1_agents.value or []
    if not _edf.is_empty() and _sel:
        _edf = _edf.filter(pl.col("agent_type").is_in(_sel))

    # DuckDB aggregations — guard against empty frames
    if not entry_df.is_empty():
        _con = duckdb.connect()
        _con.register("e", entry_df.to_arrow())
        _dur = _con.execute(
            "SELECT agent_type, ROUND(AVG(duration_ms),0) AS avg_ms,"
            " COUNT(*) AS cnt FROM e GROUP BY agent_type ORDER BY avg_ms DESC"
        ).df()
        _sts = _con.execute(
            "SELECT status, COUNT(*) AS cnt FROM e GROUP BY status ORDER BY cnt DESC"
        ).df()
        _con.close()
    else:
        _dur = pd.DataFrame(columns=["agent_type", "avg_ms", "cnt"])
        _sts = pd.DataFrame(columns=["status", "cnt"])

    p1_sess_pd = _sdf.to_pandas()
    p1_entr_pd = _edf.to_pandas() if not _edf.is_empty() else pd.DataFrame()
    p1_dur_pd  = _dur
    p1_sts_pd  = _sts
    return p1_sess_pd, p1_entr_pd, p1_dur_pd, p1_sts_pd


@app.cell
def _panel1(mo, p1_status, p1_mode, p1_agents,
            p1_sess_pd, p1_entr_pd, p1_dur_pd, p1_sts_pd):
    _st = mo.ui.table(p1_sess_pd, label="Sessions",     selection=None, page_size=8)
    _et = (mo.ui.table(p1_entr_pd, label="Step entries", selection=None, page_size=10)
           if len(p1_entr_pd) else mo.md("_No step entries_"))
    _dt = (mo.ui.table(p1_dur_pd,  label="Duration by agent", selection=None)
           if len(p1_dur_pd)  else mo.md("_No data_"))
    _ss = (mo.ui.table(p1_sts_pd,  label="Status counts",     selection=None)
           if len(p1_sts_pd)  else mo.md("_No data_"))
    panel1 = mo.vstack([
        mo.md("## 📋 Audit Trail Explorer"),
        mo.hstack([p1_status, p1_mode, p1_agents], justify="start"),
        mo.md("### Sessions"), _st,
        mo.md("### Step Entries"), _et,
        mo.md("### Aggregations"),
        mo.hstack([_dt, _ss], justify="start"),
    ])
    return (panel1,)


# ══════════════════════════════════════════════════════════════════════════════
# PANEL 2 — Registry Analytics
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
    p2_domain, p2_current,
    skill_df, registry,
    duckdb, pd, pl,
):
    """READ .value — filter and aggregate."""
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
            "SELECT domain,"
            " COUNT(*) AS total,"
            " SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current"
            " FROM s GROUP BY domain ORDER BY total DESC"
        ).df()
        _ver = _con2.execute(
            "SELECT fqsn, COUNT(*) AS versions FROM s GROUP BY fqsn ORDER BY versions DESC"
        ).df()
        _con2.close()
    else:
        _dom = pd.DataFrame(columns=["domain", "total", "current"])
        _ver = pd.DataFrame(columns=["fqsn", "versions"])

    # Task records — safe construction
    _trecs = registry.get("task_records", []) if registry else []
    if _trecs:
        try:
            _task_df = pl.from_dicts(_trecs)
            p2_task_pd = _task_df.to_pandas()
        except Exception:
            p2_task_pd = pd.DataFrame(_trecs)
    else:
        p2_task_pd = pd.DataFrame()

    p2_skill_pd = _sdf.to_pandas() if not _sdf.is_empty() else pd.DataFrame()
    p2_dom_pd   = _dom
    p2_ver_pd   = _ver
    return p2_skill_pd, p2_dom_pd, p2_ver_pd, p2_task_pd


@app.cell
def _panel2(mo, p2_domain, p2_current,
            p2_skill_pd, p2_dom_pd, p2_ver_pd, p2_task_pd):
    _d = (mo.ui.table(p2_dom_pd,   label="Domain summary",  selection=None)
          if len(p2_dom_pd)   else mo.md("_No data_"))
    _v = (mo.ui.table(p2_ver_pd,   label="Version history", selection=None)
          if len(p2_ver_pd)   else mo.md("_No data_"))
    _k = (mo.ui.table(p2_skill_pd, label="Skills", selection=None, page_size=10)
          if len(p2_skill_pd) else mo.md("_No skills_"))
    _t = (mo.ui.table(p2_task_pd,  label="Task records",    selection=None)
          if len(p2_task_pd)  else mo.md("_No task records_"))
    panel2 = mo.vstack([
        mo.md("## 🗂️ Registry Analytics"),
        mo.hstack([p2_domain, p2_current], justify="start"),
        mo.hstack([_d, _v], justify="start"),
        mo.md("### Skills"), _k,
        mo.md("### Tasks"),  _t,
    ])
    return (panel2,)


# ══════════════════════════════════════════════════════════════════════════════
# PANEL 3 — Pipeline Dashboard
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
    p3_select = mo.ui.dropdown(options=_opts, value=0, label="Select session")
    return (p3_select,)


@app.cell
def _panel3(mo, p3_select, sessions):
    """READ .value — build mermaid diagram + step table."""
    import pandas as _pd

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
            ic    = {"agent": "A", "subagent": "S", "team": "T", "python": "P"}.get(at, "?")
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
        _tbl = (mo.ui.table(_pd.DataFrame(_rows), label="Step detail", selection=None)
                if _rows else mo.md("_No steps_"))
    else:
        _cards = mo.md("_No session selected_")
        _diag  = mo.md("_Select a session above_")
        _tbl   = mo.md("_No entries_")

    panel3 = mo.vstack([
        mo.md("## 🔭 Pipeline Dashboard"),
        p3_select,
        _cards,
        mo.md("### Execution Graph"),
        _diag,
        mo.md("### Step Detail"),
        _tbl,
    ])
    return (panel3,)


# ── Final assembly ────────────────────────────────────────────────────────────

@app.cell
def _assemble(mo, panel1, panel2, panel3):
    tabs = mo.ui.tabs({
        "📋 Audit Trail": panel1,
        "🗂️ Registry":   panel2,
        "🔭 Pipeline":   panel3,
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
