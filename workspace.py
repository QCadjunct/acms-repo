"""
workspace.py

The invariant core — WorkspaceState accumulates across every node type.

Architecture Standard: Mind Over Metadata LLC — Peter Heller

ACES Parallel:
    ACES Workspace          → WorkspaceState
    ACES Task Step          → WorkspaceEntry
    ACES Workspace context  → WorkspaceState.entries (accumulated, never replaced)
    ACES Exception handler  → FailureContract
    ACES $WFLAND barrier    → TeamResult.member_entries fan-in

LangGraph integration:
    WorkspaceState IS the LangGraph State object.
    Annotated[list[WorkspaceEntry], operator.add] IS the accumulation mechanism.
    One line. No other mechanism needed.
    State accumulates. State does not replace.

Four agent types — all produce WorkspaceEntry:
    AGENT     → direct LLM call, system.md bound
    SUBAGENT  → compiled subgraph, sub_entries nested
    TEAM      → fan-out/fan-in, TeamResult with member_entries
    PYTHON    → deterministic function, tool_output captured
    BASH      → deterministic shell, stdout/stderr captured

D4 Audit Rule:
    Every step appends a WorkspaceEntry.
    Failed steps are NOT removed — RETRIED entries are ADDED.
    The audit trail is the complete history — not the happy path alone.
    The workspace IS the audit table.
"""

import hashlib
import json
import operator
from datetime import datetime, timezone
from typing import Annotated
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, computed_field

from registry.sentinels import (
    VALID_TO_OPEN_ENDED,
    PREVIOUS_HASH_ORIGIN,
    DELEGATION_NONE,
    ERROR_NONE,
    OUTPUT_NONE,
)
from registry.skills  import SkillFQSN
from registry.tasks   import TaskFQSN
from registry.status  import (
    StepStatus,
    AgentType,
    FailureStrategy,
    PromptVersion,
    TaskVersion,
    OperatingMode,
)


# ── Utilities ─────────────────────────────────────────────────────────────────

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def task_sha256(fqsn: TaskFQSN, version: TaskVersion, definition: dict) -> str:
    """
    Deterministic Task identity hash.
    Same Task + Version + Definition = same hash. Always.
    Change anything = new hash = new RegistryEvent.
    """
    definition_str = json.dumps(definition, sort_keys=True)
    return sha256(f"{fqsn.value}:{version.value}:{definition_str}")


# ── Input / Output Contracts ──────────────────────────────────────────────────

class BasePrompt(BaseModel):
    """
    The typed, validated input contract for every skill.
    ACES parallel: TDMS form definition.
    Every skill's domain prompt inherits from this.
    Only domain-specific fields vary between skills.
    """
    skill_fqsn:   SkillFQSN
    version:      PromptVersion
    submitted_at: datetime = Field(default_factory=utcnow)

    @computed_field
    @property
    def skill_hash(self) -> str:
        """SHA-256 of skill identity — derived, never entered manually."""
        return sha256(f"{self.skill_fqsn.value}:{self.version.value}")


class BaseResponse(BaseModel):
    """
    The typed, validated output contract for every skill.
    Every skill's domain response inherits from this.
    status must be a closed-set StepStatus — no free text.
    """
    skill_fqsn:   SkillFQSN
    version:      PromptVersion
    status:       StepStatus
    completed_at: datetime = Field(default_factory=utcnow)

    @computed_field
    @property
    def skill_hash(self) -> str:
        return sha256(f"{self.skill_fqsn.value}:{self.version.value}")


# ── Domain Prompts and Responses ──────────────────────────────────────────────

class ExtractPrompt(BasePrompt):
    """Input contract for DATA_EXTRACT — Step 1 Agent node."""
    raw_input:    str
    # The unstructured text to extract entities and facts from.
    target_schema: dict = Field(default_factory=dict)
    # Optional: JSON schema the extraction should conform to.


class ExtractResponse(BaseResponse):
    """Output contract for DATA_EXTRACT."""
    structured_data: dict
    # Extracted entities and facts in structured JSON.
    entity_count:    int
    confidence:      float
    # 0.0–1.0 confidence in extraction quality.


class ValidationPrompt(BasePrompt):
    """Input contract for all VALIDATION_* skills."""
    data_to_validate: dict
    # The structured data to validate.
    validation_rules: list[str] = Field(default_factory=list)
    # Optional: additional rules beyond the skill's system.md defaults.


class ValidationResponse(BaseResponse):
    """Output contract for VALIDATION_FORMAT and VALIDATION_SCHEMA leaves."""
    passed:      bool
    violations:  list[str] = Field(default_factory=list)
    # Empty list = no violations. Non-empty = list of violation descriptions.
    warnings:    list[str] = Field(default_factory=list)
    # Non-blocking issues — logged but do not fail the step.


class CompositeValidationResponse(BaseResponse):
    """Output contract for VALIDATION_COMPOSITE subagent coordinator."""
    all_passed:       bool
    format_passed:    bool
    schema_passed:    bool
    all_violations:   list[str] = Field(default_factory=list)
    # Merged violations from both format and schema validation.


class SearchPrompt(BasePrompt):
    """Input contract for SEARCH_TAVILY — team researcher member."""
    query:          str
    max_results:    int = 5
    search_depth:   str = "basic"
    # "basic" or "advanced" — Tavily API parameter.


class SearchResult(BaseModel):
    title:   str
    url:     str
    content: str
    score:   float


class SearchResponse(BaseResponse):
    """Output contract for SEARCH_TAVILY."""
    results:     list[SearchResult] = Field(default_factory=list)
    query_used:  str


class TextPrompt(BasePrompt):
    """Input contract for TEXT_TRANSFORM and TEXT_SUMMARIZE."""
    input_text:      str
    output_format:   str = "markdown"
    max_length:      int = 0
    # 0 = no limit. Positive int = max character count.


class TextResponse(BaseResponse):
    """Output contract for TEXT_TRANSFORM."""
    transformed:   str
    char_count:    int


class SummarizeResponse(BaseResponse):
    """Output contract for TEXT_SUMMARIZE."""
    summary:     str
    key_facts:   list[str] = Field(default_factory=list)
    word_count:  int


class PersistPrompt(BasePrompt):
    """Input contract for INFRA_PYTHON_PERSIST — Python tool node."""
    workspace_json: str
    # JSON-serialized WorkspaceState — persisted to PostgreSQL.
    table_name:     str = "workspace_state"


class PersistResponse(BaseResponse):
    """Output contract for INFRA_PYTHON_PERSIST."""
    rows_written: int
    task_id:      str
    session_id:   str


# ── Team Result ───────────────────────────────────────────────────────────────

class TeamMemberResult(BaseModel):
    """
    The result of one team member's execution.
    Produced by each member's subgraph before fan-in.
    """
    role:           str
    # Member role label: "researcher", "synthesizer", "formatter"

    skill_fqsn:     SkillFQSN
    # The skill this member executed.

    status:         StepStatus
    # COMPLETED, FAILED, SKIPPED — member failures are non-blocking.

    output:         str = OUTPUT_NONE
    # Member's primary output — type varies by role.

    error:          str = ERROR_NONE
    # Error description if status == FAILED. ERROR_NONE if successful.

    completed_at:   datetime = Field(default_factory=utcnow)


class TeamResult(BaseModel):
    """
    The aggregated result of all team members after fan-in.
    ACES parallel: state after $WFLAND barrier synchronization.
    """
    team_skill_fqsn:  SkillFQSN
    # The team coordinator skill: TEAM_ENRICH

    member_results:   list[TeamMemberResult] = Field(default_factory=list)
    # One TeamMemberResult per team member — all members, success or failure.

    aggregated_output: dict = Field(default_factory=dict)
    # The merged output from all successful members.
    # aggregator node responsibility — each team defines its own merge logic.

    completed_at:     datetime = Field(default_factory=utcnow)

    @property
    def all_succeeded(self) -> bool:
        return all(m.status == StepStatus.COMPLETED for m in self.member_results)

    @property
    def any_succeeded(self) -> bool:
        return any(m.status == StepStatus.COMPLETED for m in self.member_results)

    @property
    def failed_members(self) -> list[str]:
        return [m.role for m in self.member_results if m.status == StepStatus.FAILED]


# ── Workspace Entry ───────────────────────────────────────────────────────────

class WorkspaceEntry(BaseModel):
    """
    One entry per step execution — the audit table row.
    ACES parallel: one ACES workspace step record.

    Invariant:
        Append-only — never updated, never deleted.
        Failed steps preserved — RETRIED entries added alongside them.
        Sub-entries nested for subagent and team delegation.
        The complete history is always present.

    All four agent types produce a WorkspaceEntry:
        AGENT    → prompt + response, no sub_entries, no team_result
        SUBAGENT → prompt + composite_response, sub_entries populated
        TEAM     → team_result populated, sub_entries per member
        PYTHON   → tool_output populated, no prompt/response
        BASH     → tool_output populated, no prompt/response
    """
    entry_id:       UUID        = Field(default_factory=uuid4)
    valid_from:     datetime    = Field(default_factory=utcnow)
    valid_to:       datetime    = Field(default=VALID_TO_OPEN_ENDED)

    step:           int
    # 1-based position in skill_chaining. Matches StepDefinition.step.

    agent_type:     AgentType
    # Which execution pattern produced this entry.

    skill_fqsn:     SkillFQSN
    # The skill executed at this step.

    fqsn_path:      str
    # Human-readable FQSN string — for display and logging.

    fqsn_hash:      str
    # SHA-256 of fqsn + version — for audit integrity verification.

    status:         StepStatus
    # Terminal status of this step. Running entries are transient.

    retry_count:    int = 0
    # How many retries occurred before this terminal status.

    # ── Agent node fields ─────────────────────────────────────────────────────
    prompt:         BasePrompt | None = None
    # The typed input contract — populated for AGENT and SUBAGENT types.

    response:       BaseResponse | None = None
    # The typed output contract — populated for AGENT type.

    # ── Subagent fields ───────────────────────────────────────────────────────
    delegated_to:   str = DELEGATION_NONE
    # DELEGATION_NONE = not delegated.
    # Non-empty = FQSN of subagent or team that handled this step.

    sub_entries:    list["WorkspaceEntry"] = Field(default_factory=list)
    # The subagent's own WorkspaceEntry list — nested audit trail.
    # Parent sees the complete internal chain — nothing hidden.

    # ── Team fields ───────────────────────────────────────────────────────────
    team_result:    TeamResult | None = None
    # Populated for TEAM agent type — contains all member results + aggregation.

    # ── Tool node fields ──────────────────────────────────────────────────────
    tool_output:    str = OUTPUT_NONE
    # Raw output from PYTHON or BASH tool nodes.

    tool_exit_code: int = 0
    # Exit code from BASH tool nodes. 0 = success. Non-zero = failure.

    # ── Error ────────────────────────────────────────────────────────────────
    error:          str = ERROR_NONE
    # Error description if status == FAILED or TIMEOUT.
    # ERROR_NONE if step succeeded.

    # ── Timing ───────────────────────────────────────────────────────────────
    started_at:     datetime = Field(default_factory=utcnow)
    completed_at:   datetime = Field(default_factory=utcnow)

    @property
    def duration_ms(self) -> float:
        """Execution duration in milliseconds."""
        delta = self.completed_at - self.started_at
        return delta.total_seconds() * 1000

    @property
    def has_delegation(self) -> bool:
        return self.delegated_to != DELEGATION_NONE

    @property
    def has_sub_entries(self) -> bool:
        return len(self.sub_entries) > 0

    @property
    def is_tool_node(self) -> bool:
        return self.agent_type in (AgentType.PYTHON, AgentType.BASH)


# ── Failure Contract ──────────────────────────────────────────────────────────

class FailureContract(BaseModel):
    """
    The governed failure response for one step.
    Lives in the spec — not in the code.
    ACES parallel: the exception path definition in the Task Definition Language.

    LangGraph implementation:
        FailureContract governs the conditional edge predicate.
        The predicate reads WorkspaceState.last_entry().status.
        If FAILED → evaluate FailureContract.strategy.
        strategy == RETRY_STEP    → loop back to this step node.
        strategy == SKIP_STEP     → advance to next step node.
        strategy == FAIL_TASK     → route to END with FAILED status.
        strategy == ESCALATE      → route to signal_notify node (Phase 2).
    """
    step:             int
    strategy:         FailureStrategy
    max_retries:      int = 3
    retry_delay_sec:  int = 5
    skip_to_step:     int | None = None
    # Only relevant if strategy == SKIP_STEP.
    # None = skip to next step in sequence.
    # Non-None = skip to specific step number.

    escalation_message: str = ""
    # Human-readable message for Signal notification.
    # Only relevant if strategy == ESCALATE (Phase 2).


# ── Workspace State ───────────────────────────────────────────────────────────

class WorkspaceState(BaseModel):
    """
    The LangGraph State object — the sole state carrier.
    ACES parallel: the ACES Workspace + audit table combined.

    Accumulation invariant:
        entries is Annotated with operator.add.
        LangGraph ADDS new entries — never replaces.
        This one annotation IS the entire accumulation mechanism.
        Failed entries stay. Retried entries are added alongside them.
        The complete history is always present.

    LangGraph StateGraph usage:
        graph = StateGraph(WorkspaceState)
        Nodes receive WorkspaceState, return WorkspaceState.
        LangGraph merges returned state via operator.add on entries.
        Messages merged via add_messages.
    """

    # ── Task identity ─────────────────────────────────────────────────────────
    task_id:        str
    # SHA-256(task_fqsn + task_version + definition) — deterministic.

    task_fqsn:      TaskFQSN
    task_version:   TaskVersion
    task_path:      str
    # Human-readable task path — for display and logging.

    operating_mode: OperatingMode = OperatingMode.MAAS
    # From TaskRegistry.operating_mode — governs Split Trust Boundary.

    # ── Session ───────────────────────────────────────────────────────────────
    session_id:     UUID = Field(default_factory=uuid4)
    # Unique identifier for this execution instance.

    # ── Accumulated audit trail ───────────────────────────────────────────────
    entries: Annotated[list[WorkspaceEntry], operator.add] = Field(
        default_factory=list
    )
    # THE accumulation field.
    # operator.add means LangGraph APPENDS new entries.
    # This is the entire audit trail — all steps, all statuses, all retries.
    # ACES parallel: the workspace record table.

    # ── Current execution context ─────────────────────────────────────────────
    current_step:   int = 1
    # The step currently executing — 1-based.

    current_skill:  SkillFQSN | None = None
    # The skill currently bound to the executing node.

    current_prompt: dict = Field(default_factory=dict)
    # Transport container — holds the typed prompt as dict for inter-node passing.
    # Each node casts dict → its own domain Prompt type on receipt.

    # ── Failure tracking ──────────────────────────────────────────────────────
    error_count:    int = 0
    # Total failures across all steps — incremented on each FAILED entry.

    retry_counts:   dict[int, int] = Field(default_factory=dict)
    # Per-step retry count — {step_number: retry_count}.
    # FailureContract reads this to decide RETRY vs FAIL.

    failure_contracts: list[FailureContract] = Field(default_factory=list)
    # The governed failure spec — loaded from TaskRegistry at graph construction.
    # Conditional edges read this — not hardcoded if/else in node functions.

    # ── Team state ────────────────────────────────────────────────────────────
    active_team_results: dict[str, TeamResult] = Field(default_factory=dict)
    # In-flight team results keyed by team skill FQSN.
    # Populated during fan-out, consumed by aggregator at fan-in.

    # ── Timing ───────────────────────────────────────────────────────────────
    created_at:     datetime = Field(default_factory=utcnow)
    completed_at:   datetime | None = None

    # ── Computed properties ───────────────────────────────────────────────────

    @computed_field
    @property
    def is_failed(self) -> bool:
        """True if any BLOCKING step has failed and exhausted retries."""
        return any(
            e.status == StepStatus.FAILED
            for e in self.entries
            if e.agent_type != AgentType.TEAM
        )

    @computed_field
    @property
    def last_entry(self) -> WorkspaceEntry | None:
        """Most recent WorkspaceEntry — available to next node."""
        return self.entries[-1] if self.entries else None

    @computed_field
    @property
    def last_output(self) -> str:
        """Most recent non-empty output — convenience accessor for chaining."""
        for entry in reversed(self.entries):
            if entry.status == StepStatus.COMPLETED:
                if entry.response and hasattr(entry.response, "transformed"):
                    return entry.response.transformed
                if entry.tool_output != OUTPUT_NONE:
                    return entry.tool_output
        return OUTPUT_NONE

    def failed_steps(self) -> list[WorkspaceEntry]:
        """All entries with FAILED status — for audit and retry logic."""
        return [e for e in self.entries if e.status == StepStatus.FAILED]

    def retry_count_for_step(self, step: int) -> int:
        """Current retry count for a given step number."""
        return self.retry_counts.get(step, 0)

    def failure_contract_for_step(self, step: int) -> FailureContract | None:
        """Retrieve the governed failure contract for a step."""
        for fc in self.failure_contracts:
            if fc.step == step:
                return fc
        return None

    def audit_summary(self) -> dict:
        """
        Compact audit view — mirrors a workspace_state table query.
        Suitable for logging, Signal notifications, and LangSmith tracing.
        """
        return {
            "task_id":      self.task_id,
            "task_path":    self.task_path,
            "session_id":   str(self.session_id),
            "step_count":   len(self.entries),
            "error_count":  self.error_count,
            "is_failed":    self.is_failed,
            "entries": [
                {
                    "step":        e.step,
                    "agent_type":  e.agent_type.value,
                    "skill":       e.fqsn_path,
                    "status":      e.status.value,
                    "retry_count": e.retry_count,
                    "duration_ms": round(e.duration_ms, 2),
                    "delegated_to": e.delegated_to,
                    "sub_entry_count": len(e.sub_entries),
                }
                for e in self.entries
            ],
        }


# ── Workspace Factory ─────────────────────────────────────────────────────────

def create_workspace(
    task_fqsn:        TaskFQSN,
    task_version:     TaskVersion,
    definition:       dict,
    failure_contracts: list[FailureContract] | None = None,
    operating_mode:   OperatingMode = OperatingMode.MAAS,
) -> WorkspaceState:
    """
    Factory — creates a typed, hashed, audit-ready WorkspaceState.
    The task_id SHA-256 is derived from fqsn + version + definition.
    Same inputs = same hash. Change anything = new hash.

    failure_contracts loaded from TaskRegistry at graph construction time.
    They live in the spec — not hardcoded in node functions.
    """
    return WorkspaceState(
        task_id           = task_sha256(task_fqsn, task_version, definition),
        task_fqsn         = task_fqsn,
        task_version      = task_version,
        task_path         = task_fqsn.value,
        operating_mode    = operating_mode,
        failure_contracts = failure_contracts or [],
    )
