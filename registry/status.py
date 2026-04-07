"""
registry/status.py

Closed-set enums — replaces every free-text string in the system.
Pydantic rejects any value not in the set at model instantiation time.

Architecture Standard: Mind Over Metadata LLC — Peter Heller
    D4 equivalent: named CHECK constraints on every status column.
    No free-text status strings anywhere in the system.
    Every status is a governed, versioned, documented value.
    Adding a status = adding an enum member = a deliberate architectural decision.

D4 parallel:
    StepStatus       = CHECK CONSTRAINT on workspace_entry.status
    AgentType        = CHECK CONSTRAINT on workspace_entry.agent_type
    FailureStrategy  = CHECK CONSTRAINT on task_definition.failure_strategy
    OperatingMode    = CHECK CONSTRAINT on task_definition.operating_mode
"""

from enum import Enum


# ── Step Execution Status ─────────────────────────────────────────────────────

class StepStatus(str, Enum):
    """
    The execution status of one WorkspaceEntry.
    D4: named CHECK constraint on workspace_entry.status column.
    """
    PENDING    = "pending"
    # Step is declared in skill_chaining but not yet reached.
    # Initial state for all steps at task creation.

    RUNNING    = "running"
    # Step is actively executing — LLM call in flight or tool running.
    # Written at node entry, replaced by terminal status at node exit.

    COMPLETED  = "completed"
    # Step completed successfully — output contract fulfilled.
    # Happy path terminal state.

    FAILED     = "failed"
    # Step failed — error captured in WorkspaceEntry.error.
    # Triggers failure_contract evaluation.

    RETRIED    = "retried"
    # Step failed and was retried — retry count incremented.
    # Previous FAILED entry preserved in audit trail — never overwritten.
    # New entry appended with RETRIED status.

    DELEGATED  = "delegated"
    # Step was handed to a subagent or team — execution is internal.
    # Parent WorkspaceEntry.delegated_to = FQSN of subagent/team.
    # Sub-entries nested in WorkspaceEntry.sub_entries.

    SKIPPED    = "skipped"
    # Step was skipped by conditional edge — failure_contract decision.
    # Recorded in audit trail — not invisible.

    TIMEOUT    = "timeout"
    # Step exceeded max execution time — treated as FAILED for retry logic.


# ── Agent Type ────────────────────────────────────────────────────────────────

class AgentType(str, Enum):
    """
    The execution pattern of one WorkspaceEntry.
    D4: named CHECK constraint on workspace_entry.agent_type column.
    Determines how the node is constructed and how state flows through it.
    """
    AGENT      = "agent"
    # Single LLM call — one skill, one system.md, one node.
    # Simplest case. Input → LLM → Output. WorkspaceEntry appended.
    # ACES parallel: single task step.

    SUBAGENT   = "subagent"
    # Compiled subgraph — own StateGraph, own skill_chaining.
    # Parent graph sees it as atomic — internal chain invisible to parent.
    # Sub-entries nested in WorkspaceEntry.sub_entries.
    # ACES parallel: Task-Call-Task — subtask invocation.

    TEAM       = "team"
    # Parallel fan-out/fan-in — multiple members, own skill_chaining per member.
    # Fan-out: all members execute in parallel via StateGraph parallel branches.
    # Fan-in: aggregator node collects all member outputs.
    # ACES parallel: $WFLAND barrier — parallel steps synchronized at barrier.

    PYTHON     = "python"
    # Deterministic Python function — no LLM call.
    # Governed by function signature, not system.md.
    # Still produces WorkspaceEntry + RegistryEvent — first-class audit citizen.
    # Examples: hash computation, database persist, file I/O, API call.

    BASH       = "bash"
    # Deterministic shell execution — no LLM call.
    # Governed by command template + allowed_commands allowlist.
    # stdout/stderr captured. exit code checked.
    # ACES parallel: DCL command procedure step.


# ── Failure Strategy ──────────────────────────────────────────────────────────

class FailureStrategy(str, Enum):
    """
    How the task responds to a step failure.
    D4: named CHECK constraint on task_definition.failure_strategy column.
    Governs the conditional edge behavior — spec not code.
    """
    RETRY_STEP        = "retry_step"
    # Retry the failed step — max_retries times with retry_delay_sec backoff.
    # Previous FAILED entry preserved. New RETRIED entry appended.

    RETRY_FROM_START  = "retry_from_start"
    # Restart the entire task from Step 1.
    # Used when step failure invalidates all prior outputs.

    SKIP_STEP         = "skip_step"
    # Skip the failed step and continue to next.
    # Used for non-blocking optional enrichment steps.
    # SKIPPED entry appended to audit trail.

    FAIL_TASK         = "fail_task"
    # Abort the entire task — propagate failure to caller.
    # Used for blocking required steps (e.g., INFRA_PYTHON_PERSIST).

    ESCALATE          = "escalate"
    # Send Signal notification + await human-in-the-loop decision.
    # Phase 2 — requires INFRA_SIGNAL_NOTIFY skill.


# ── Operating Mode ────────────────────────────────────────────────────────────

class OperatingMode(str, Enum):
    """
    Which infrastructure layer executes this task.
    D4: named CHECK constraint on task_definition.operating_mode column.
    Governs Split Trust Boundary — spec not hardcode.
    See: Split-Trust-Boundary.md
    """
    CLOUD      = "cloud"
    # Full public cloud — AWS/Azure/GCP.
    # Stateless inference only. No sensitive data crosses boundary.

    MAAS       = "maas"
    # Full MaaS — FreedomTower/TheBeast/MiniBeast cluster.
    # All data stays on cluster. Ollama inference. Zero egress cost.

    HYBRID     = "hybrid"
    # Split Trust Boundary active.
    # Cloud: stateless inference, Tavily search, LangSmith tracing.
    # MaaS: PostgreSQL, pgvector, WAL, FQSN registry, sensitive data.


# ── Version Enums ─────────────────────────────────────────────────────────────

class SkillVersion(str, Enum):
    V1_0_0 = "1.0.0"
    V1_1_0 = "1.1.0"
    V2_0_0 = "2.0.0"


class TaskVersion(str, Enum):
    V1_0_0 = "1.0.0"
    V1_1_0 = "1.1.0"
    V2_0_0 = "2.0.0"


class PromptVersion(str, Enum):
    V1_0_0 = "1.0.0"
    V1_1_0 = "1.1.0"
    V2_0_0 = "2.0.0"
