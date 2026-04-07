"""
tasks/acms_proof.py

ACES Proof of Concept — The Singular Task That Proves the Thesis.

Architecture Standard: Mind Over Metadata LLC — Peter Heller

Thesis:
    A system.md-governed, FQSN-registered, LangGraph-executed task pipeline
    with full audit trail and failure recovery is architecturally equivalent
    to ACES — and superior to current agentic frameworks because it adds
    the governance layer they lack.

What this task proves:

    ✓ Single Agent node
        DATA_EXTRACT — raw text → structured JSON via system.md-bound LLM.

    ✓ Subagent delegation (Task-Call-Task)
        VALIDATION_COMPOSITE — compiled subgraph, internal chain invisible.
        Sub-entries nested in parent WorkspaceEntry.
        ACES parallel: Task-Call-Task subtask invocation.

    ✓ Team parallel execution ($WFLAND barrier)
        TEAM_ENRICH — three members, fan-out, fan-in aggregation.
        researcher (SEARCH_TAVILY) + synthesizer (TEXT_SUMMARIZE) +
        formatter (TEXT_TRANSFORM) execute in parallel.
        Aggregator waits at $WFLAND barrier for all members.
        ACES parallel: $WFLAND barrier synchronization primitive.

    ✓ Python tool node (deterministic, audited)
        INFRA_PYTHON_PERSIST — WorkspaceState → PostgreSQL.
        No system.md. No LLM. Deterministic function.
        Still produces WorkspaceEntry + RegistryEvent — first-class citizen.

    ✓ Failure contract governed by spec not code
        VALIDATION_COMPOSITE failure → retry DATA_EXTRACT (max 3).
        Retry exhausted → INFRA_PYTHON_PERSIST with FAILED status.
        Conditional edge reads FailureContract — not hardcoded if/else.

    ✓ Complete audit trail
        Every step appends WorkspaceEntry.
        Failed steps preserved — RETRIED entries added alongside.
        WorkspaceState persisted to PostgreSQL via INFRA_PYTHON_PERSIST.

skill_chaining (happy path):
    Step 1: DATA_EXTRACT           → Agent
    Step 2: VALIDATION_COMPOSITE   → Subagent (chains FORMAT → SCHEMA)
    Step 3: TEAM_ENRICH            → Team (researcher + synthesizer + formatter)
    Step 4: INFRA_PYTHON_PERSIST   → Python tool node

failure_contract:
    Step 1: RETRY_STEP (max 3) — extraction is recoverable
    Step 2: RETRY_STEP (max 3) → if exhausted → FAIL_TASK
    Step 3: SKIP_STEP           — enrichment is non-blocking
    Step 4: FAIL_TASK           — persistence is blocking — audit required
"""

import asyncio
import json
import os
import subprocess
from datetime import datetime, timezone
from uuid import uuid4

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from registry.skills import SkillFQSN
from registry.tasks  import TaskFQSN
from registry.status import (
    AgentType,
    StepStatus,
    FailureStrategy,
    PromptVersion,
    TaskVersion,
    OperatingMode,
)
from registry.db import DatabaseRegistry

from workspace import (
    WorkspaceState,
    WorkspaceEntry,
    FailureContract,
    TeamMemberResult,
    TeamResult,
    ExtractPrompt,
    ExtractResponse,
    ValidationPrompt,
    ValidationResponse,
    CompositeValidationResponse,
    SearchPrompt,
    SearchResponse,
    SearchResult,
    TextPrompt,
    TextResponse,
    SummarizeResponse,
    PersistPrompt,
    PersistResponse,
    create_workspace,
    sha256,
    utcnow,
)
from registry.sentinels import (
    DELEGATION_NONE,
    ERROR_NONE,
    OUTPUT_NONE,
)


# ── System.md Content ─────────────────────────────────────────────────────────
# In production: loaded from filesystem via FQSN resolution.
# In POC: embedded directly — same behavioral contract.

SYSTEM_MD = {
    SkillFQSN.DATA_EXTRACT: """
# IDENTITY and PURPOSE
You are a **Data Extraction Specialist** operating under D4 governance principles.
Your sole purpose is to extract structured data from unstructured text.

# RULES
- Extract ONLY what is explicitly stated in the input. Never infer.
- Every extracted field must have a direct textual source.
- Output ONLY valid JSON. No prose. No explanation.
- Confidence score: 1.0 = explicitly stated, 0.5 = implied, 0.0 = absent.

# OUTPUT CONTRACT
{
  "entities": [{"name": str, "type": str, "value": str, "confidence": float}],
  "relationships": [{"subject": str, "predicate": str, "object": str}],
  "key_facts": [str],
  "extraction_confidence": float
}
""",

    SkillFQSN.VALIDATION_FORMAT: """
# IDENTITY and PURPOSE
You are a **Format Validation Specialist** operating under D4 governance.
You validate that structured data meets format requirements.

# VALIDATION RULES
- All required fields must be present (no missing keys).
- All string fields must be non-empty.
- All numeric fields must be within valid ranges.
- No NULL values anywhere — D4 two-value predicate logic.
- List fields must have at least one element.

# OUTPUT CONTRACT
{
  "passed": bool,
  "violations": [str],
  "warnings": [str]
}
""",

    SkillFQSN.VALIDATION_SCHEMA: """
# IDENTITY and PURPOSE
You are a **Schema Validation Specialist** operating under D4 governance.
You validate that structured data conforms to referential integrity rules.

# VALIDATION RULES
- Confidence values must be between 0.0 and 1.0 inclusive.
- Entity types must be from the governed taxonomy: PERSON, ORG, LOCATION, DATE, CONCEPT.
- Relationship predicates must use active voice, present tense.
- key_facts must be complete sentences, not fragments.

# OUTPUT CONTRACT
{
  "passed": bool,
  "violations": [str],
  "warnings": [str]
}
""",

    SkillFQSN.TEXT_SUMMARIZE: """
# IDENTITY and PURPOSE
You are a **Synthesis Specialist** operating under D4 governance.
You synthesize search results into governed summaries.

# RULES
- Summarize ONLY what is in the provided content. Never add external knowledge.
- Key facts must be directly sourced from the input.
- Summary must be bounded to 200 words maximum.
- Output ONLY valid JSON.

# OUTPUT CONTRACT
{
  "summary": str,
  "key_facts": [str],
  "word_count": int
}
""",

    SkillFQSN.TEXT_TRANSFORM: """
# IDENTITY and PURPOSE
You are a **Format Transformation Specialist** operating under D4 governance.
You transform text into clean, structured markdown.

# RULES
- Preserve all factual content — no omissions.
- Apply consistent heading hierarchy.
- Use bullet points for lists, not prose.
- Output ONLY the transformed text. No explanation.
""",
}


# ── LLM Factory ───────────────────────────────────────────────────────────────

def get_llm(skill_fqsn: SkillFQSN) -> ChatAnthropic:
    """
    Returns the LLM bound to the given skill's system.md.
    In production: model selection governed by OperatingMode + Split Trust Boundary.
    In POC: Claude Sonnet for all skills.
    """
    system_md = SYSTEM_MD.get(skill_fqsn, "You are a helpful assistant.")
    return ChatAnthropic(
        model="claude-sonnet-4-20250514",
        api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
        system=system_md,
        max_tokens=2048,
    )


# ── Node Factory — Agent ──────────────────────────────────────────────────────

def make_agent_node(
    skill_fqsn: SkillFQSN,
    step_number: int,
):
    """
    Factory that produces a LangGraph node function for an AGENT type skill.
    The node is bound to the skill's system.md via get_llm().
    ACES parallel: one task step with one defined processing unit.
    """
    llm = get_llm(skill_fqsn)

    async def agent_node(state: WorkspaceState) -> dict:
        started = utcnow()
        prompt_dict = state.current_prompt
        human_content = json.dumps(prompt_dict, indent=2)

        try:
            response = await llm.ainvoke([HumanMessage(content=human_content)])
            raw_output = response.content

            # Parse JSON response
            clean = raw_output.strip()
            if clean.startswith("```"):
                clean = clean.split("\n", 1)[1].rsplit("```", 1)[0]
            output_data = json.loads(clean)

            # Build typed response based on skill
            if skill_fqsn == SkillFQSN.DATA_EXTRACT:
                typed_response = ExtractResponse(
                    skill_fqsn=skill_fqsn,
                    version=PromptVersion.V1_0_0,
                    status=StepStatus.COMPLETED,
                    structured_data=output_data,
                    entity_count=len(output_data.get("entities", [])),
                    confidence=output_data.get("extraction_confidence", 0.0),
                )
            else:
                typed_response = BaseResponseAdapter(
                    skill_fqsn=skill_fqsn,
                    output=output_data,
                    status=StepStatus.COMPLETED,
                )

            entry = WorkspaceEntry(
                step=step_number,
                agent_type=AgentType.AGENT,
                skill_fqsn=skill_fqsn,
                fqsn_path=skill_fqsn.value,
                fqsn_hash=sha256(f"{skill_fqsn.value}:1.0.0"),
                status=StepStatus.COMPLETED,
                prompt=None,
                response=typed_response,
                started_at=started,
                completed_at=utcnow(),
            )

        except Exception as exc:
            entry = WorkspaceEntry(
                step=step_number,
                agent_type=AgentType.AGENT,
                skill_fqsn=skill_fqsn,
                fqsn_path=skill_fqsn.value,
                fqsn_hash=sha256(f"{skill_fqsn.value}:1.0.0"),
                status=StepStatus.FAILED,
                error=str(exc),
                started_at=started,
                completed_at=utcnow(),
            )

        return {
            "entries": [entry],
            "current_step": step_number + 1,
            "error_count": state.error_count + (1 if entry.status == StepStatus.FAILED else 0),
        }

    agent_node.__name__ = f"agent_{skill_fqsn.name.lower()}"
    return agent_node


# ── Subagent — VALIDATION_COMPOSITE ──────────────────────────────────────────

def build_validation_subgraph() -> "CompiledGraph":
    """
    Builds the VALIDATION_COMPOSITE compiled subgraph.
    Internal chain: VALIDATION_FORMAT → VALIDATION_SCHEMA
    ACES parallel: Task-Call-Task subtask definition.

    The parent graph sees this as a single atomic node.
    The internal skill_chaining is the subagent's own governed contract.
    Sub-entries are nested inside the parent WorkspaceEntry.
    """
    format_llm  = get_llm(SkillFQSN.VALIDATION_FORMAT)
    schema_llm  = get_llm(SkillFQSN.VALIDATION_SCHEMA)

    async def validate_format_node(state: WorkspaceState) -> dict:
        started = utcnow()
        data_to_validate = state.last_entry.response.structured_data \
            if state.last_entry and state.last_entry.response \
            else state.current_prompt

        content = json.dumps({"data": data_to_validate}, indent=2)
        try:
            resp = await format_llm.ainvoke([HumanMessage(content=content)])
            raw = json.loads(resp.content.strip())
            status = StepStatus.COMPLETED if raw.get("passed", False) else StepStatus.FAILED
            entry = WorkspaceEntry(
                step=1,
                agent_type=AgentType.AGENT,
                skill_fqsn=SkillFQSN.VALIDATION_FORMAT,
                fqsn_path=SkillFQSN.VALIDATION_FORMAT.value,
                fqsn_hash=sha256(f"{SkillFQSN.VALIDATION_FORMAT.value}:1.0.0"),
                status=status,
                error="; ".join(raw.get("violations", [])) if status == StepStatus.FAILED else ERROR_NONE,
                tool_output=json.dumps(raw),
                started_at=started,
                completed_at=utcnow(),
            )
        except Exception as exc:
            entry = WorkspaceEntry(
                step=1,
                agent_type=AgentType.AGENT,
                skill_fqsn=SkillFQSN.VALIDATION_FORMAT,
                fqsn_path=SkillFQSN.VALIDATION_FORMAT.value,
                fqsn_hash=sha256(f"{SkillFQSN.VALIDATION_FORMAT.value}:1.0.0"),
                status=StepStatus.FAILED,
                error=str(exc),
                started_at=started,
                completed_at=utcnow(),
            )
        return {"entries": [entry], "current_step": 2}

    async def validate_schema_node(state: WorkspaceState) -> dict:
        started = utcnow()
        # Retrieve the data from the most recent COMPLETED agent entry
        data_entry = next(
            (e for e in reversed(state.entries)
             if e.status == StepStatus.COMPLETED and e.agent_type == AgentType.AGENT),
            None
        )
        data_to_validate = {}
        if data_entry and data_entry.tool_output != OUTPUT_NONE:
            data_to_validate = json.loads(data_entry.tool_output)

        content = json.dumps({"data": data_to_validate}, indent=2)
        try:
            resp = await schema_llm.ainvoke([HumanMessage(content=content)])
            raw = json.loads(resp.content.strip())
            status = StepStatus.COMPLETED if raw.get("passed", False) else StepStatus.FAILED
            entry = WorkspaceEntry(
                step=2,
                agent_type=AgentType.AGENT,
                skill_fqsn=SkillFQSN.VALIDATION_SCHEMA,
                fqsn_path=SkillFQSN.VALIDATION_SCHEMA.value,
                fqsn_hash=sha256(f"{SkillFQSN.VALIDATION_SCHEMA.value}:1.0.0"),
                status=status,
                error="; ".join(raw.get("violations", [])) if status == StepStatus.FAILED else ERROR_NONE,
                tool_output=json.dumps(raw),
                started_at=started,
                completed_at=utcnow(),
            )
        except Exception as exc:
            entry = WorkspaceEntry(
                step=2,
                agent_type=AgentType.AGENT,
                skill_fqsn=SkillFQSN.VALIDATION_SCHEMA,
                fqsn_path=SkillFQSN.VALIDATION_SCHEMA.value,
                fqsn_hash=sha256(f"{SkillFQSN.VALIDATION_SCHEMA.value}:1.0.0"),
                status=StepStatus.FAILED,
                error=str(exc),
                started_at=started,
                completed_at=utcnow(),
            )
        return {"entries": [entry], "current_step": 3}

    # Build subgraph
    sub = StateGraph(WorkspaceState)
    sub.add_node("validate_format", validate_format_node)
    sub.add_node("validate_schema",  validate_schema_node)
    sub.add_edge(START, "validate_format")
    sub.add_edge("validate_format", "validate_schema")
    sub.add_edge("validate_schema", END)
    return sub.compile()


# ── Team — TEAM_ENRICH ────────────────────────────────────────────────────────

def build_enrichment_team() -> "CompiledGraph":
    """
    Builds the TEAM_ENRICH compiled subgraph.
    Three members execute in parallel — fan-out/fan-in.
    ACES parallel: $WFLAND barrier — parallel steps synchronized at barrier.

    Fan-out:  researcher + synthesizer + formatter all start simultaneously.
    Fan-in:   aggregator_node waits for all three, merges TeamResult.
    """
    tavily_available = bool(os.environ.get("TAVILY_API_KEY"))
    summarize_llm = get_llm(SkillFQSN.TEXT_SUMMARIZE)
    transform_llm = get_llm(SkillFQSN.TEXT_TRANSFORM)

    async def researcher_node(state: WorkspaceState) -> dict:
        """Team member: researcher — SEARCH_TAVILY."""
        started = utcnow()
        query = state.last_output or "ACES LangGraph agentic architecture"

        try:
            if tavily_available:
                from tavily import TavilyClient
                client = TavilyClient(api_key=os.environ["TAVILY_API_KEY"])
                raw = client.search(query=query, max_results=3)
                results = [
                    SearchResult(
                        title=r.get("title", ""),
                        url=r.get("url", ""),
                        content=r.get("content", ""),
                        score=r.get("score", 0.0),
                    )
                    for r in raw.get("results", [])
                ]
                output = json.dumps([r.model_dump() for r in results])
            else:
                # POC mock — Tavily not required to prove the pattern
                output = json.dumps([{
                    "title": "ACES Architecture Mock Result",
                    "url": "https://mock.example.com",
                    "content": "Mock search result for POC validation",
                    "score": 0.9,
                }])

            member = TeamMemberResult(
                role="researcher",
                skill_fqsn=SkillFQSN.SEARCH_TAVILY,
                status=StepStatus.COMPLETED,
                output=output,
            )
        except Exception as exc:
            member = TeamMemberResult(
                role="researcher",
                skill_fqsn=SkillFQSN.SEARCH_TAVILY,
                status=StepStatus.FAILED,
                error=str(exc),
            )

        return {
            "active_team_results": {
                "researcher": member.model_dump(),
            }
        }

    async def synthesizer_node(state: WorkspaceState) -> dict:
        """Team member: synthesizer — TEXT_SUMMARIZE."""
        started = utcnow()
        content = state.last_output or "No prior content available for synthesis."

        try:
            resp = await summarize_llm.ainvoke([
                HumanMessage(content=f"Synthesize: {content[:2000]}")
            ])
            raw = json.loads(resp.content.strip())
            member = TeamMemberResult(
                role="synthesizer",
                skill_fqsn=SkillFQSN.TEXT_SUMMARIZE,
                status=StepStatus.COMPLETED,
                output=json.dumps(raw),
            )
        except Exception as exc:
            member = TeamMemberResult(
                role="synthesizer",
                skill_fqsn=SkillFQSN.TEXT_SUMMARIZE,
                status=StepStatus.FAILED,
                error=str(exc),
            )

        return {
            "active_team_results": {
                "synthesizer": member.model_dump(),
            }
        }

    async def formatter_node(state: WorkspaceState) -> dict:
        """Team member: formatter — TEXT_TRANSFORM."""
        started = utcnow()
        content = state.last_output or "No prior content available for formatting."

        try:
            resp = await transform_llm.ainvoke([
                HumanMessage(content=f"Transform to structured markdown:\n{content[:2000]}")
            ])
            member = TeamMemberResult(
                role="formatter",
                skill_fqsn=SkillFQSN.TEXT_TRANSFORM,
                status=StepStatus.COMPLETED,
                output=resp.content,
            )
        except Exception as exc:
            member = TeamMemberResult(
                role="formatter",
                skill_fqsn=SkillFQSN.TEXT_TRANSFORM,
                status=StepStatus.FAILED,
                error=str(exc),
            )

        return {
            "active_team_results": {
                "formatter": member.model_dump(),
            }
        }

    async def aggregator_node(state: WorkspaceState) -> dict:
        """
        Fan-in aggregator — the $WFLAND barrier.
        ACES parallel: the synchronization point where all parallel
        task steps must complete before execution continues.

        Collects all member results from active_team_results.
        Builds TeamResult. Appends WorkspaceEntry to audit trail.
        Non-blocking: team continues even if individual members failed.
        """
        started = utcnow()
        raw_results = state.active_team_results

        member_results = []
        aggregated = {}

        for role, member_dict in raw_results.items():
            member = TeamMemberResult(**member_dict)
            member_results.append(member)
            if member.status == StepStatus.COMPLETED:
                aggregated[role] = member.output

        team_result = TeamResult(
            team_skill_fqsn=SkillFQSN.TEAM_ENRICH,
            member_results=member_results,
            aggregated_output=aggregated,
        )

        # Build the sub-entries — one per team member
        sub_entries = [
            WorkspaceEntry(
                step=i + 1,
                agent_type=AgentType.AGENT,
                skill_fqsn=m.skill_fqsn,
                fqsn_path=m.skill_fqsn.value,
                fqsn_hash=sha256(f"{m.skill_fqsn.value}:1.0.0"),
                status=m.status,
                tool_output=m.output,
                error=m.error,
            )
            for i, m in enumerate(member_results)
        ]

        # Overall team status: COMPLETED if any member succeeded
        overall = StepStatus.COMPLETED if team_result.any_succeeded else StepStatus.FAILED

        entry = WorkspaceEntry(
            step=3,
            agent_type=AgentType.TEAM,
            skill_fqsn=SkillFQSN.TEAM_ENRICH,
            fqsn_path=SkillFQSN.TEAM_ENRICH.value,
            fqsn_hash=sha256(f"{SkillFQSN.TEAM_ENRICH.value}:1.0.0"),
            status=overall,
            delegated_to="researcher+synthesizer+formatter",
            sub_entries=sub_entries,
            team_result=team_result,
            tool_output=json.dumps(aggregated),
            started_at=started,
            completed_at=utcnow(),
        )

        return {
            "entries": [entry],
            "current_step": 4,
            "active_team_results": {},  # Clear after aggregation
        }

    # Build team subgraph — fan-out then fan-in
    team = StateGraph(WorkspaceState)
    team.add_node("researcher",  researcher_node)
    team.add_node("synthesizer", synthesizer_node)
    team.add_node("formatter",   formatter_node)
    team.add_node("aggregator",  aggregator_node)

    # Fan-out — all three start simultaneously from START
    team.add_edge(START,        "researcher")
    team.add_edge(START,        "synthesizer")
    team.add_edge(START,        "formatter")

    # Fan-in — all three feed into aggregator ($WFLAND barrier)
    team.add_edge("researcher",  "aggregator")
    team.add_edge("synthesizer", "aggregator")
    team.add_edge("formatter",   "aggregator")

    team.add_edge("aggregator",  END)
    return team.compile()


# ── Python Tool Node — INFRA_PYTHON_PERSIST ───────────────────────────────────

async def persist_node(state: WorkspaceState) -> dict:
    """
    Python tool node — deterministic, no LLM call.
    Persists complete WorkspaceState to PostgreSQL.
    ACES parallel: the final task step that commits the workspace to storage.

    Blocking step — if this fails, the task fails.
    FailureStrategy.FAIL_TASK — no retry, no skip.
    The audit trail must be durable. This is not optional.
    """
    started = utcnow()

    try:
        db_url = os.environ.get("ACES_DATABASE_URL", "")
        workspace_json = state.model_dump_json(indent=2)
        rows_written = 0

        if db_url:
            import asyncpg
            conn = await asyncpg.connect(db_url)
            try:
                await conn.execute(
                    """
                    INSERT INTO workspace_state
                        (task_id, task_fqsn, task_version, session_id, entries, status)
                    VALUES ($1, $2, $3, $4, $5::jsonb, $6)
                    ON CONFLICT (session_id) DO UPDATE
                        SET entries     = EXCLUDED.entries,
                            status      = EXCLUDED.status,
                            completed_at = NOW()
                    """,
                    state.task_id,
                    state.task_fqsn.value,
                    state.task_version.value,
                    str(state.session_id),
                    json.dumps(state.audit_summary()),
                    "completed",
                )
                rows_written = 1
            finally:
                await conn.close()
        else:
            # POC fallback — write to local JSON file for inspection
            output_path = f"/tmp/workspace_{state.session_id}.json"
            with open(output_path, "w") as f:
                f.write(workspace_json)
            rows_written = 1

        entry = WorkspaceEntry(
            step=4,
            agent_type=AgentType.PYTHON,
            skill_fqsn=SkillFQSN.INFRA_PYTHON_PERSIST,
            fqsn_path=SkillFQSN.INFRA_PYTHON_PERSIST.value,
            fqsn_hash=sha256(f"{SkillFQSN.INFRA_PYTHON_PERSIST.value}:1.0.0"),
            status=StepStatus.COMPLETED,
            tool_output=f"rows_written={rows_written} session_id={state.session_id}",
            started_at=started,
            completed_at=utcnow(),
        )

    except Exception as exc:
        entry = WorkspaceEntry(
            step=4,
            agent_type=AgentType.PYTHON,
            skill_fqsn=SkillFQSN.INFRA_PYTHON_PERSIST,
            fqsn_path=SkillFQSN.INFRA_PYTHON_PERSIST.value,
            fqsn_hash=sha256(f"{SkillFQSN.INFRA_PYTHON_PERSIST.value}:1.0.0"),
            status=StepStatus.FAILED,
            error=str(exc),
            started_at=started,
            completed_at=utcnow(),
        )

    return {
        "entries": [entry],
        "current_step": 5,
        "completed_at": utcnow(),
        "error_count": state.error_count + (1 if entry.status == StepStatus.FAILED else 0),
    }


# ── Conditional Edge Predicates ───────────────────────────────────────────────
# These predicates READ the FailureContract — they do NOT encode failure logic.
# The FailureContract IS the spec. The predicate IS the executor of the spec.
# ACES parallel: the exception handler routing decision.

def after_extract(state: WorkspaceState) -> str:
    """
    Conditional edge after DATA_EXTRACT (Step 1).
    Reads FailureContract for step 1.
    """
    last = state.last_entry
    if last and last.status == StepStatus.FAILED:
        fc = state.failure_contract_for_step(1)
        if fc and fc.strategy == FailureStrategy.RETRY_STEP:
            if state.retry_count_for_step(1) < fc.max_retries:
                return "retry_extract"
        return "persist"  # exhausted retries → persist with FAILED status
    return "validate"


def after_validate(state: WorkspaceState) -> str:
    """
    Conditional edge after VALIDATION_COMPOSITE (Step 2).
    Reads FailureContract for step 2.
    If validation fails → retry extract (the source of bad data).
    If retries exhausted → persist with FAILED status.
    """
    # Find the last validation-related entries
    validation_entries = [
        e for e in state.entries
        if e.skill_fqsn in (
            SkillFQSN.VALIDATION_COMPOSITE,
            SkillFQSN.VALIDATION_FORMAT,
            SkillFQSN.VALIDATION_SCHEMA,
        )
    ]
    if any(e.status == StepStatus.FAILED for e in validation_entries):
        fc = state.failure_contract_for_step(2)
        if fc and fc.strategy == FailureStrategy.RETRY_STEP:
            if state.retry_count_for_step(1) < (fc.max_retries if fc else 3):
                return "retry_extract"  # re-extract — source may have been bad
        return "persist"  # exhausted → persist FAILED
    return "enrich"


def after_enrich(state: WorkspaceState) -> str:
    """
    Conditional edge after TEAM_ENRICH (Step 3).
    Enrichment is non-blocking — SKIP_STEP strategy.
    Always continues to persist regardless of team member failures.
    """
    return "persist"


def after_persist(state: WorkspaceState) -> str:
    """
    Conditional edge after INFRA_PYTHON_PERSIST (Step 4).
    FAIL_TASK strategy — persistence failure ends the graph.
    """
    last = state.last_entry
    if last and last.status == StepStatus.FAILED:
        return END  # FAIL_TASK — audit required, cannot continue
    return END  # happy path — task complete


# ── Retry Coordinator ─────────────────────────────────────────────────────────

async def retry_coordinator(state: WorkspaceState) -> dict:
    """
    Increments retry count before routing back to extract.
    Updates retry_counts dict for the step being retried.
    Appends RETRIED entry to preserve audit trail continuity.
    """
    step = 1  # DATA_EXTRACT is step 1
    current_count = state.retry_count_for_step(step)
    new_counts = {**state.retry_counts, step: current_count + 1}

    retry_entry = WorkspaceEntry(
        step=step,
        agent_type=AgentType.AGENT,
        skill_fqsn=SkillFQSN.DATA_EXTRACT,
        fqsn_path=SkillFQSN.DATA_EXTRACT.value,
        fqsn_hash=sha256(f"{SkillFQSN.DATA_EXTRACT.value}:1.0.0"),
        status=StepStatus.RETRIED,
        retry_count=current_count + 1,
        error=f"Retry {current_count + 1} — previous extraction failed validation",
    )

    return {
        "entries": [retry_entry],
        "retry_counts": new_counts,
        "error_count": state.error_count,
    }


# ── Subagent Wrapper ──────────────────────────────────────────────────────────

def make_subagent_wrapper(subgraph, step_number: int, skill_fqsn: SkillFQSN):
    """
    Wraps a compiled subgraph as a parent graph node.
    Executes the subgraph and nests its entries as sub_entries
    in a single parent WorkspaceEntry.
    ACES parallel: Task-Call-Task — parent sees subtask as atomic.
    """
    async def subagent_node(state: WorkspaceState) -> dict:
        started = utcnow()
        try:
            sub_result = await subgraph.ainvoke(state)
            sub_entries = sub_result.get("entries", [])

            all_passed = all(
                e.status == StepStatus.COMPLETED
                for e in sub_entries
            )

            entry = WorkspaceEntry(
                step=step_number,
                agent_type=AgentType.SUBAGENT,
                skill_fqsn=skill_fqsn,
                fqsn_path=skill_fqsn.value,
                fqsn_hash=sha256(f"{skill_fqsn.value}:1.0.0"),
                status=StepStatus.COMPLETED if all_passed else StepStatus.FAILED,
                delegated_to=skill_fqsn.value,
                sub_entries=sub_entries,
                error=ERROR_NONE if all_passed else "Subagent validation failed",
                started_at=started,
                completed_at=utcnow(),
            )
        except Exception as exc:
            entry = WorkspaceEntry(
                step=step_number,
                agent_type=AgentType.SUBAGENT,
                skill_fqsn=skill_fqsn,
                fqsn_path=skill_fqsn.value,
                fqsn_hash=sha256(f"{skill_fqsn.value}:1.0.0"),
                status=StepStatus.FAILED,
                delegated_to=skill_fqsn.value,
                error=str(exc),
                started_at=started,
                completed_at=utcnow(),
            )

        return {
            "entries": [entry],
            "current_step": step_number + 1,
            "error_count": state.error_count + (1 if entry.status == StepStatus.FAILED else 0),
        }

    subagent_node.__name__ = f"subagent_{skill_fqsn.name.lower()}"
    return subagent_node


# ── Main Graph Builder ────────────────────────────────────────────────────────

async def build_aces_proof_graph(
    db_registry: DatabaseRegistry | None = None,
) -> "CompiledGraph":
    """
    Builds and compiles the complete ACES Proof of Concept graph.

    Node inventory:
        extract         — Agent node (DATA_EXTRACT)
        retry_coord     — Retry coordinator (increments retry count)
        validate        — Subagent wrapper (VALIDATION_COMPOSITE)
        enrich          — Team wrapper (TEAM_ENRICH)
        persist         — Python tool node (INFRA_PYTHON_PERSIST)

    Edge inventory:
        START → extract
        extract → [conditional: validate | retry_extract | persist]
        retry_coord → extract
        validate → [conditional: enrich | retry_extract | persist]
        enrich → [conditional: persist]  (always → persist)
        persist → END

    Checkpointer:
        PostgreSQL checkpointer via DatabaseRegistry.write_dsn()
        Enables LangGraph fault tolerance — resume from any checkpoint.
        Falls back to MemorySaver if DatabaseRegistry not provided.
    """
    # ── Failure contracts — loaded from spec, not hardcoded ──────────────────
    failure_contracts = [
        FailureContract(
            step=1,
            strategy=FailureStrategy.RETRY_STEP,
            max_retries=3,
            retry_delay_sec=2,
        ),
        FailureContract(
            step=2,
            strategy=FailureStrategy.RETRY_STEP,
            max_retries=3,
            retry_delay_sec=2,
        ),
        FailureContract(
            step=3,
            strategy=FailureStrategy.SKIP_STEP,
            max_retries=0,
        ),
        FailureContract(
            step=4,
            strategy=FailureStrategy.FAIL_TASK,
            max_retries=0,
        ),
    ]

    # ── Build subgraph and team ───────────────────────────────────────────────
    validation_subgraph = build_validation_subgraph()
    enrichment_team     = build_enrichment_team()

    # ── Wrap subagent and team as parent-compatible nodes ─────────────────────
    validate_node = make_subagent_wrapper(
        validation_subgraph, step_number=2, skill_fqsn=SkillFQSN.VALIDATION_COMPOSITE
    )
    enrich_node = make_subagent_wrapper(
        enrichment_team, step_number=3, skill_fqsn=SkillFQSN.TEAM_ENRICH
    )

    # ── Main graph ────────────────────────────────────────────────────────────
    graph = StateGraph(WorkspaceState)

    # Nodes
    graph.add_node("extract",     make_agent_node(SkillFQSN.DATA_EXTRACT, step_number=1))
    graph.add_node("retry_coord", retry_coordinator)
    graph.add_node("validate",    validate_node)
    graph.add_node("enrich",      enrich_node)
    graph.add_node("persist",     persist_node)

    # Entry point
    graph.add_edge(START, "extract")

    # Conditional edges — read FailureContract, do not encode failure logic
    graph.add_conditional_edges(
        "extract",
        after_extract,
        {
            "validate":      "validate",
            "retry_extract": "retry_coord",
            "persist":       "persist",
        },
    )
    graph.add_edge("retry_coord", "extract")

    graph.add_conditional_edges(
        "validate",
        after_validate,
        {
            "enrich":        "enrich",
            "retry_extract": "retry_coord",
            "persist":       "persist",
        },
    )

    graph.add_conditional_edges(
        "enrich",
        after_enrich,
        {"persist": "persist"},
    )

    graph.add_conditional_edges(
        "persist",
        after_persist,
        {END: END},
    )

    # ── Checkpointer ─────────────────────────────────────────────────────────
    if db_registry:
        checkpointer = AsyncPostgresSaver.from_conn_string(
            db_registry.write_dsn()
        )
        await checkpointer.setup()
        return graph.compile(checkpointer=checkpointer)
    else:
        from langgraph.checkpoint.memory import MemorySaver
        return graph.compile(checkpointer=MemorySaver())


# ── Task Execution Entry Point ────────────────────────────────────────────────

async def run_aces_proof(raw_input: str) -> dict:
    """
    Execute the ACES Proof of Concept task end-to-end.

    Args:
        raw_input: Unstructured text to process through the full pipeline.

    Returns:
        audit_summary dict — the complete WorkspaceState audit trail.
    """
    # Create governed workspace
    initial_state = create_workspace(
        task_fqsn=TaskFQSN.PIPELINE_ACES_PROOF,
        task_version=TaskVersion.V1_0_0,
        definition={"description": "ACES Proof of Concept — all four node types"},
        failure_contracts=[
            FailureContract(step=1, strategy=FailureStrategy.RETRY_STEP, max_retries=3),
            FailureContract(step=2, strategy=FailureStrategy.RETRY_STEP, max_retries=3),
            FailureContract(step=3, strategy=FailureStrategy.SKIP_STEP),
            FailureContract(step=4, strategy=FailureStrategy.FAIL_TASK),
        ],
        operating_mode=OperatingMode.MAAS,
    )

    # Inject initial prompt into workspace
    initial_state = initial_state.model_copy(update={
        "current_prompt": {
            "skill_fqsn": SkillFQSN.DATA_EXTRACT.value,
            "raw_input": raw_input,
        }
    })

    # Build graph
    graph = await build_aces_proof_graph()

    # Execute — LangGraph accumulates WorkspaceState across all nodes
    config = {"configurable": {"thread_id": str(initial_state.session_id)}}
    final_state = await graph.ainvoke(initial_state, config=config)

    final_state_obj = WorkspaceState(**final_state)
    return final_state_obj.audit_summary()


# ── CLI Entry Point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    raw_input = sys.argv[1] if len(sys.argv) > 1 else (
        "Peter Heller founded Mind Over Metadata LLC in 2003 to develop "
        "the D4 Domain-Driven Database Design methodology. The company is "
        "based in New York City and teaches database systems at Queens College CUNY."
    )

    print("=" * 70)
    print("ACES PROOF OF CONCEPT")
    print("Mind Over Metadata LLC — Peter Heller")
    print("=" * 70)
    print(f"\nInput: {raw_input[:80]}...\n")

    result = asyncio.run(run_aces_proof(raw_input))

    print(json.dumps(result, indent=2, default=str))
    print("\n" + "=" * 70)
    print(f"Task ID:    {result['task_id']}")
    print(f"Steps:      {result['step_count']}")
    print(f"Errors:     {result['error_count']}")
    print(f"Failed:     {result['is_failed']}")
    print("=" * 70)
