# aces-repo

> **DEC VAX ACES architecture patterns applied to LangGraph agentic orchestration.**  
> Spec-governed. FQSN-registered. Fully audited. Task-oriented.  
> © 2026 Mind Over Metadata LLC — Peter Heller

---

## The Thesis

In 1985, DEC shipped ACES — the Application Control and Management System — for VAX/VMS. It solved high-volume transaction orchestration with a governed task pipeline: defined steps, controlled delegation, parallel execution barriers, deterministic failure recovery, and a complete audit workspace. Every behavior was declared in a spec. Nothing was hardcoded.

In 2024, the agentic AI community is solving the same problem with LangGraph. Most implementations hardcode providers, routing, and failure behavior directly into graph construction code. They lack the governance layer ACES had forty years ago.

This repository proves the thesis:

> **A `system.md`-governed, FQSN-registered, LangGraph-executed task pipeline is architecturally equivalent to ACES — and superior to current agentic frameworks because it adds the governance layer they lack.**

---

## What This Proves

| Proof | ACES Concept | LangGraph Implementation |
|---|---|---|
| Single agent execution | Task step | `AgentType.AGENT` node |
| Task-Call-Task delegation | Subtask invocation | `AgentType.SUBAGENT` compiled subgraph |
| Parallel execution + barrier | `$WFLAND` barrier | `AgentType.TEAM` fan-out/fan-in |
| Deterministic tool execution | DCL command step | `AgentType.PYTHON` tool node |
| Failure recovery by spec | Exception handler | `FailureContract` + conditional edge |
| Complete audit trail | Workspace record | `WorkspaceState` accumulation → PostgreSQL |
| Governed behavioral contract | Task Definition Language | `system.md` + `SkillRegistry` |
| Versioned skill catalog | ADF App Definition File | `TaskRegistry` + FQSN hash chain |

---

## Architecture

```
Ignition Key = system.md + TaskRegistry + SkillFQSN registry
             ↓
LangGraph StateGraph (WorkspaceState accumulates — never replaces)
             ↓
    Step 1: DATA_EXTRACT           AgentType.AGENT
    Step 2: VALIDATION_COMPOSITE   AgentType.SUBAGENT  → [FORMAT → SCHEMA]
    Step 3: TEAM_ENRICH            AgentType.TEAM      → [researcher ‖ synthesizer ‖ formatter]
    Step 4: INFRA_PYTHON_PERSIST   AgentType.PYTHON
             ↓
PostgreSQL — WorkspaceState persisted, audit trail durable
```

---

## Naming Convention

This codebase follows the **ACES Naming Convention Standard**:

- **Explicit over cryptic** — `DatabaseRegistry` not `DbRegistry`
- **Self-documenting nouns** — `SkillRegistry` not `SkillSat`
- **No abbreviations** in class or module names
- **No external methodology vocabulary** — D4 Temporal Registry Pattern, not Data Vault 2
- **Governance travels with the artifact** — temporal contracts live in the model, not the name

---

## D4 Principles Applied

| Principle | Implementation |
|---|---|
| Two-value predicate logic | `valid_to == VALID_TO_OPEN_ENDED` = current. No NULL. |
| Allen Interval temporal RI | `valid_from` / `valid_to` on every registry record |
| Append-only audit | `WorkspaceEntry` never updated — failed entries preserved |
| Hash chain provenance | SHA-256 per `SkillRegistry` version — break = integrity violation |
| Named CHECK constraints | All status columns use closed-set enums — `FailureStrategy`, `AgentType`, `OperatingMode` |
| Governance as code | `FailureContract` lives in the spec — not in `if/else` node logic |
| State changes only | `RegistryEvent` fires ONLY on hash delta — identical hash = silent pass |

---

## Repository Structure

```
acms-langgraph-poc/
├── registry/
│   ├── __init__.py         Public API — import from here
│   ├── sentinels.py        D4 Named Defaults — VALID_TO_OPEN_ENDED, PREVIOUS_HASH_ORIGIN
│   ├── skills.py           SkillFQSN enum — 20 governed skill paths
│   ├── tasks.py            TaskFQSN enum — 8 governed task paths
│   ├── status.py           Closed-set enums — StepStatus, AgentType, FailureStrategy
│   ├── records.py          SkillRegistry + TaskRegistry — D4 Temporal Registry Pattern
│   └── db.py               DatabaseRegistry — three-node PostgreSQL HA + DDL
├── tasks/
│   └── acms_proof.py       THE POC — all four node types, failure contract, audit trail
├── system_md/              One system.md per skill — the behavioral contracts
├── workspace.py            WorkspaceState — the accumulation engine
├── docs/                   Architecture diagrams
├── tests/                  pytest test suite
├── requirements.txt        Astral uv toolchain
└── README.md               This file
```

---

## Quickstart

```bash
# Install — Astral uv toolchain
uv pip install -r requirements.txt

# Environment
export ANTHROPIC_API_KEY=your_key
export ACES_DATABASE_URL=postgresql://acms:password@localhost:5432/acms_registry
# ACES_DATABASE_URL is optional — POC writes to /tmp if not set

# Run the proof
python tasks/acms_proof.py "Peter Heller founded Mind Over Metadata LLC in 2003 \
to develop the D4 Domain-Driven Database Design methodology."

# Run tests
uvx pytest tests/ -v
```

---

## The ACES Parallel — Explicit

```
DEC VAX ACES (1985)              This Repository (2026)
────────────────────             ──────────────────────────────
Task Definition Language    ←→   TaskRegistry.skill_chaining
Task Step                   ←→   StepDefinition + Node function
Workspace Context           ←→   WorkspaceState (accumulates)
Task-Call-Task              ←→   AgentType.SUBAGENT compiled subgraph
$WFLAND Barrier             ←→   AgentType.TEAM fan-out/fan-in + aggregator
Exception Handler           ←→   FailureContract + conditional edge
Execution Controller        ←→   LangGraph CompiledGraph runtime
ADF App Definition File     ←→   TaskRegistry
TDMS Form Definition        ←→   BasePrompt / BaseResponse
DCL Command Step            ←→   AgentType.PYTHON / AgentType.BASH
```

The architecture is identical. The vocabulary changed. The pattern is 40 years old. Most of the agentic community is rediscovering it one painful lesson at a time.

---

## Methodology

Built on the **Navigator + Driver** AI pair programming model.  
Published: [The Navigator and the Driver — Medium](https://medium.com/@peterheller)  
Methodology: **D4 Domain-Driven Database Design** — Mind Over Metadata LLC  
Teaching: CSCI 331 / CSCI 381 — Queens College CUNY

---

## License

Code: MIT License  
D4 Methodology, Navigator + Driver, FQSN taxonomy: © 2026 Mind Over Metadata LLC — Peter Heller  
All rights reserved on methodology artifacts.
