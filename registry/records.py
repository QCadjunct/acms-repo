"""
registry/records.py

D4 Temporal Registry Records — governed, versioned, hash-chained.

Architecture Standard: Mind Over Metadata LLC — Peter Heller

    SkillRegistry — one row per skill version.
    TaskRegistry  — one row per task version.

Naming standard:
    SkillRegistry / TaskRegistry — D4 native, registry-cohesive vocabulary.
    NOT SkillSatellite / TaskSatellite — no DV2 terminology in this codebase.
    See: ACES-Naming-Convention-Standard.md

D4 Temporal Pattern (NOT Data Vault 2):
    valid_from / valid_to    ← Allen Interval temporal referential integrity
    VALID_TO_OPEN_ENDED      ← two-value predicate logic — current record sentinel
    previous_hash            ← hash chain provenance — break = integrity violation
    Append-only              ← never UPDATE, never DELETE — D4 audit rule
    State changes only       ← RegistryEvent fires ONLY on hash delta

D4 vs DV2 (explicit distinction):
    DV2 requires Hub + Satellite + Link table separation.
    D4 Temporal Registry collapses this into a single governed model.
    The temporal and provenance contracts live IN the model — not the name.
    This is appropriate for a POC. Production would split into proper tables.
"""

from datetime import datetime, timezone
from pydantic import BaseModel, Field

from registry.sentinels import (
    VALID_TO_OPEN_ENDED,
    PREVIOUS_HASH_ORIGIN,
    DIFF_ORIGIN,
)
from registry.skills  import SkillFQSN
from registry.tasks   import TaskFQSN
from registry.status  import (
    SkillVersion,
    TaskVersion,
    OperatingMode,
    FailureStrategy,
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ── Skill Registry ────────────────────────────────────────────────────────────

class SkillRegistry(BaseModel):
    """
    One row per skill version in the D4 Temporal Registry.

    Hash chain invariant:
        origin:     previous_hash == PREVIOUS_HASH_ORIGIN ("")
        subsequent: previous_hash == prior version's fqsn_hash
        Break in chain = integrity violation = RegistryEvent(INTEGRITY_VIOLATION)

    Temporal invariant:
        current:    valid_to == VALID_TO_OPEN_ENDED
        historical: valid_to <  VALID_TO_OPEN_ENDED
        No NULL. No ambiguity. Two-value predicate logic only.

    system.md contract:
        system_md is the full resolved content of the skill's system.md file.
        fqsn_hash = SHA-256(fqsn + version + system_md content)
        Change one character in system.md = new hash = new version = RegistryEvent.
    """

    # ── Business key — never changes ──────────────────────────────────────────
    fqsn:             SkillFQSN
    # The Fully Qualified Skill Name — closed set, stable identifier.
    # Example: SkillFQSN.DATA_EXTRACT = "skills/data/extract"

    # ── Version identity ──────────────────────────────────────────────────────
    version:          SkillVersion
    # Semantic version — closed set. Adding a version = deliberate decision.

    fqsn_hash:        str
    # SHA-256(fqsn.value + ":" + version.value + ":" + system_md_content)
    # Deterministic. Same inputs = same hash. Change anything = new hash.
    # This IS the integrity seal for this skill version.

    previous_hash:    str = PREVIOUS_HASH_ORIGIN
    # "" = origin version (no predecessor).
    # Non-empty = hash of prior SkillRegistry row for this fqsn.
    # Chain verification: this.previous_hash == prior.fqsn_hash

    # ── Content snapshot — stored as JSONB in PostgreSQL ─────────────────────
    system_md:        str
    # Full content of system.md at this version.
    # This is the behavioral contract — the Ignition Key component 1.
    # Stored verbatim — not a file path, not a reference. The content itself.

    user_md:          str = ""
    # Full content of user.md if present. "" = not present.
    # user.md provides context that system.md should not contain.

    tool_binding:     str = ""
    # For PYTHON and BASH agent types: the function/command identifier.
    # "" = LLM agent (governed by system_md).
    # Non-empty = deterministic tool (governed by function signature).

    diff:             dict = Field(default_factory=lambda: dict(DIFF_ORIGIN))
    # {} = origin version (no diff).
    # Non-empty = structured diff from prior version.
    # Keys: added[], removed[], changed[{field, from, to}]

    # ── Allen Interval — temporal validity ───────────────────────────────────
    valid_from:       datetime = Field(default_factory=utcnow)
    valid_to:         datetime = Field(default=VALID_TO_OPEN_ENDED)
    # D4 Allen Interval: [valid_from, valid_to)
    # valid_to == VALID_TO_OPEN_ENDED → this IS the current version.
    # To supersede: set valid_to = utcnow() on old row, insert new row.
    # Never UPDATE the content. Never DELETE. Append only.

    # ── Provenance ────────────────────────────────────────────────────────────
    created_at:       datetime = Field(default_factory=utcnow)
    deprecated_at:    datetime = Field(default=VALID_TO_OPEN_ENDED)
    # deprecated_at != VALID_TO_OPEN_ENDED → skill is deprecated.
    # A deprecated skill's valid_to is also set. Both sentinel fields close together.

    # ── Computed properties ───────────────────────────────────────────────────

    @property
    def is_current(self) -> bool:
        """True if this is the current version of this skill."""
        return self.valid_to == VALID_TO_OPEN_ENDED

    @property
    def is_origin(self) -> bool:
        """True if this is the first version — no predecessor in hash chain."""
        return self.previous_hash == PREVIOUS_HASH_ORIGIN

    @property
    def is_deprecated(self) -> bool:
        """True if this skill has been deprecated."""
        return self.deprecated_at != VALID_TO_OPEN_ENDED

    @property
    def is_tool_node(self) -> bool:
        """True if this skill is a deterministic tool (Python/Bash) not an LLM agent."""
        return bool(self.tool_binding)

    def verify_chain(self, prior: "SkillRegistry") -> bool:
        """
        Verify hash chain integrity between this version and its predecessor.
        Returns True if chain is intact. False = integrity violation.
        """
        return self.previous_hash == prior.fqsn_hash


# ── Task Registry ─────────────────────────────────────────────────────────────

class SubSkillChaining(BaseModel):
    """
    Delegation contract for a subagent or team member.
    Maps a role name to an ordered list of skills it executes.

    Subagent example:
        role: "validation_composite"
        skills: [VALIDATION_FORMAT, VALIDATION_SCHEMA]

    Team member example:
        role: "researcher"
        skills: [SEARCH_TAVILY]
    """
    role:         str
    # The role name — unique within the task.
    # For subagent: the subagent's FQSN string.
    # For team member: the member's role label (researcher, synthesizer, formatter).

    skills:       list[SkillFQSN]
    # Ordered skill chain for this role — executed sequentially within the role.
    # Empty list = role has no skills (configuration error — caught at validation).

    is_parallel:  bool = False
    # False = sequential execution (subagent pattern).
    # True  = parallel execution (team member pattern — fan-out).

    failure_strategy: FailureStrategy = FailureStrategy.RETRY_STEP
    # How this role's chain responds to step failure.
    # Team members default SKIP_STEP (non-blocking).
    # Subagents default RETRY_STEP (blocking — parent waits).


class StepDefinition(BaseModel):
    """
    One step in the task's skill_chaining.
    The governed definition of what happens at each position in the workflow.
    """
    step:             int
    # Position in skill_chaining — 1-based. Matches WorkspaceEntry.step.

    skill_fqsn:       SkillFQSN
    # The skill executed at this step.

    description:      str
    # Human-readable description of what this step does.
    # Self-documenting — aligns with ACES Naming Convention Standard.

    failure_strategy: FailureStrategy
    # How this step responds to failure — governed by spec not code.

    max_retries:      int = 3
    # Maximum retry attempts before failure_strategy escalates.
    # Only relevant if failure_strategy == RETRY_STEP.

    retry_delay_sec:  int = 5
    # Seconds to wait between retries — simple backoff.

    is_blocking:      bool = True
    # True  = next step waits for this step to complete.
    # False = next step starts regardless (fire-and-forget).
    # Most steps are blocking. Team members are non-blocking within the team.

    delegation:       SubSkillChaining | None = None
    # None = this step is a direct Agent or tool node.
    # Non-None = this step delegates to a subagent or team.
    # The SubSkillChaining defines the internal chain.


class TaskRegistry(BaseModel):
    """
    One row per task version in the D4 Temporal Registry.

    The task IS the workflow. The skill_chaining IS the spec.
    Change the chain = new version = new hash = RegistryEvent fires.

    skill_chaining invariant:
        Ordered list of StepDefinition objects.
        Each step declares its skill, failure strategy, and delegation.
        The happy path is the ordered sequence.
        The failure contract is the failure_strategy on each step.
        Subagent and team delegation is the delegation field.
        No separate workflow file. No separate config. The task IS the spec.

    ACES parallel:
        TaskRegistry      = ADF (Application Definition File)
        skill_chaining    = Task Definition Language step sequence
        StepDefinition    = individual task step with exception handler
        SubSkillChaining  = subtask definition (Task-Call-Task)
        failure_strategy  = exception path routing
    """

    # ── Business key — never changes ──────────────────────────────────────────
    fqsn:             TaskFQSN

    # ── Version identity ──────────────────────────────────────────────────────
    version:          TaskVersion

    fqsn_hash:        str
    # SHA-256(fqsn.value + ":" + version.value + ":" + JSON(skill_chaining))
    # Changing ANY step definition = new hash = new version.

    previous_hash:    str = PREVIOUS_HASH_ORIGIN

    # ── Workflow definition — the core of the task ────────────────────────────
    description:      str
    # Self-documenting task description. Mandatory. No empty descriptions.

    operating_mode:   OperatingMode = OperatingMode.MAAS
    # Which infrastructure layer executes this task.
    # Governs Split Trust Boundary — spec not hardcode.

    skill_chaining:   list[StepDefinition]
    # The ordered workflow — the happy path.
    # Each StepDefinition is a governed step with failure contract.
    # This IS the spec. This IS the ACES Task Definition Language equivalent.

    # ── Metadata ──────────────────────────────────────────────────────────────
    definition:       dict = Field(default_factory=dict)
    # JSONB snapshot of the full task at this version — for audit queries.

    diff:             dict = Field(default_factory=lambda: dict(DIFF_ORIGIN))

    # ── Allen Interval ────────────────────────────────────────────────────────
    valid_from:       datetime = Field(default_factory=utcnow)
    valid_to:         datetime = Field(default=VALID_TO_OPEN_ENDED)

    # ── Provenance ────────────────────────────────────────────────────────────
    created_at:       datetime = Field(default_factory=utcnow)
    deprecated_at:    datetime = Field(default=VALID_TO_OPEN_ENDED)

    # ── Computed properties ───────────────────────────────────────────────────

    @property
    def is_current(self) -> bool:
        return self.valid_to == VALID_TO_OPEN_ENDED

    @property
    def is_origin(self) -> bool:
        return self.previous_hash == PREVIOUS_HASH_ORIGIN

    @property
    def step_count(self) -> int:
        return len(self.skill_chaining)

    @property
    def has_subagents(self) -> bool:
        """True if any step delegates to a subagent."""
        return any(
            s.delegation is not None and not s.delegation.is_parallel
            for s in self.skill_chaining
        )

    @property
    def has_teams(self) -> bool:
        """True if any step delegates to a team (parallel fan-out)."""
        return any(
            s.delegation is not None and s.delegation.is_parallel
            for s in self.skill_chaining
        )

    @property
    def has_tool_nodes(self) -> bool:
        """True if any step is a Python or Bash tool node."""
        from registry.skills import SkillFQSN as SF
        tool_skills = {SF.INFRA_PYTHON_PERSIST, SF.INFRA_PYTHON_HASH, SF.INFRA_BASH_EXEC}
        return any(s.skill_fqsn in tool_skills for s in self.skill_chaining)

    def get_step(self, step_number: int) -> StepDefinition | None:
        """Retrieve a step by its 1-based position number."""
        for step in self.skill_chaining:
            if step.step == step_number:
                return step
        return None

    def verify_chain(self, prior: "TaskRegistry") -> bool:
        """Verify hash chain integrity with predecessor."""
        return self.previous_hash == prior.fqsn_hash
