"""
registry/skills.py

Global Skill Registry — Fully Qualified Skill Names (FQSN).
The filesystem path IS the identifier. The enum value IS the FQSN.

Architecture Standard: Mind Over Metadata LLC — Peter Heller
    FQSN structure: skills/{domain}/{subdomain}/{name}
    Adding a skill    = adding an enum member + creating system.md
    Deprecating a skill = setting valid_to in SkillRegistry — never deleting
    Four-tool resolution: Claude Code / Fabric / Codex / Gemini

FQSN Taxonomy (three levels):
    L1 — domain         (data, validation, search, text, infra, team)
    L2 — subdomain      (extract, format, schema, tavily, transform, python)
    L3 — name           (leaf — the skill itself)

POC Skills (Phase 1 — proves the thesis):
    skills/data/extract                 ← Agent node — raw input → structured JSON
    skills/validation/format            ← Subagent leaf — format rules
    skills/validation/schema            ← Subagent leaf — schema rules
    skills/validation/composite         ← Subagent coordinator — chains format+schema
    skills/search/tavily                ← Team member — web search
    skills/text/transform               ← Team member — text formatting
    skills/team/enrich                  ← Team coordinator — fan-out/fan-in
    skills/infra/python/persist         ← Python tool node — write to PostgreSQL
"""

from enum import Enum


class SkillFQSN(str, Enum):

    # ── Data Skills ───────────────────────────────────────────────────────────
    # Domain: data | Purpose: ingest, extract, structure, validate raw inputs

    DATA_EXTRACT           = "skills/data/extract"
    # Transforms raw unstructured text into structured JSON.
    # POC role: Step 1 — the entry point Agent node.
    # system.md: Extract entities, relationships, and key facts.
    # Output contract: ExtractResponse with structured_data: dict

    DATA_STRUCTURE         = "skills/data/structure"
    # Imposes a target schema on loosely structured data.
    # Output contract: StructureResponse with schema_validated: bool

    DATA_VALIDATE          = "skills/data/validate"
    # Validates structured data against D4 two-value predicate logic rules.
    # No NULL, no ambiguous states, no open-ended strings where enums apply.

    DATA_ENRICH            = "skills/data/enrich"
    # Adds derived fields, lookups, and computed values.

    # ── Validation Skills ─────────────────────────────────────────────────────
    # Domain: validation | Purpose: governed quality gates
    # Subagent pattern: composite chains format + schema

    VALIDATION_FORMAT      = "skills/validation/format"
    # Validates format rules: field presence, type correctness, length constraints.
    # POC role: Subagent leaf — Step 1 of validation composite.
    # Output contract: ValidationResponse with passed: bool, violations: list[str]

    VALIDATION_SCHEMA      = "skills/validation/schema"
    # Validates schema rules: referential integrity, enum membership, range checks.
    # POC role: Subagent leaf — Step 2 of validation composite.
    # Output contract: ValidationResponse with passed: bool, violations: list[str]

    VALIDATION_COMPOSITE   = "skills/validation/composite"
    # Subagent coordinator — chains VALIDATION_FORMAT → VALIDATION_SCHEMA.
    # POC role: Step 2 — the Subagent node in the main graph.
    # Internal sub_skill_chaining: [VALIDATION_FORMAT, VALIDATION_SCHEMA]
    # Parent graph sees this as atomic — internal chain invisible.
    # Output contract: CompositeValidationResponse with all_passed: bool

    VALIDATION_SEMANTIC    = "skills/validation/semantic"
    # Validates semantic correctness — meaning, consistency, domain rules.

    # ── Search Skills ─────────────────────────────────────────────────────────
    # Domain: search | Purpose: external retrieval

    SEARCH_TAVILY          = "skills/search/tavily"
    # Web search via Tavily API — returns structured search results.
    # POC role: Team member "researcher" in enrichment team.
    # Cloud side of Split Trust Boundary — stateless, no sensitive data.
    # Output contract: SearchResponse with results: list[SearchResult]

    SEARCH_SEMANTIC        = "skills/search/semantic"
    # pgvector HNSW semantic search — MaaS side, stays on cluster.
    # Queries replica-2 PostgreSQL with pgvector extension.

    # ── Text Skills ───────────────────────────────────────────────────────────
    # Domain: text | Purpose: language processing and transformation

    TEXT_TRANSFORM         = "skills/text/transform"
    # Reformats, restructures, or rewrites text per output contract.
    # POC role: Team member "formatter" in enrichment team.
    # Also used standalone as an Agent node.
    # Output contract: TextResponse with transformed: str

    TEXT_SUMMARIZE         = "skills/text/summarize"
    # Produces a governed summary — length-bounded, key-facts preserving.
    # POC role: Team member "synthesizer" in enrichment team.
    # Output contract: TextResponse with summary: str, key_facts: list[str]

    TEXT_CLASSIFY          = "skills/text/classify"
    # Assigns closed-set category labels from a governed taxonomy.
    # Output contract: ClassifyResponse with category: str, confidence: float

    TEXT_VALIDATE          = "skills/text/validate"
    # Validates text against linguistic rules and output format contracts.

    # ── Infrastructure Skills ─────────────────────────────────────────────────
    # Domain: infra | Purpose: deterministic tool execution
    # NOT LLM calls — deterministic Python/Bash execution nodes
    # Still produce WorkspaceEntry + RegistryEvent — first-class audit citizens

    INFRA_PYTHON_PERSIST   = "skills/infra/python/persist"
    # Persists WorkspaceState to PostgreSQL primary.
    # POC role: Step 4 — the Python tool node. Proves durable audit trail.
    # No system.md — governed by function signature, not LLM prompt.
    # Output contract: PersistResponse with rows_written: int, task_id: str

    INFRA_PYTHON_HASH      = "skills/infra/python/hash"
    # Computes SHA-256 hash of any artifact — skill, task, workspace, document.
    # Used by SkillDeltaScanner for change detection.

    INFRA_BASH_EXEC        = "skills/infra/bash/exec"
    # Executes a governed shell command — output captured, exit code checked.
    # system.md replaced by: command template + allowed_commands allowlist.

    INFRA_SIGNAL_NOTIFY    = "skills/infra/signal/notify"
    # Sends Signal message via signal-cli-rest-api gateway.
    # Human-in-the-loop notifications and approval gates.
    # Phase 2 — defined now, implemented later.

    # ── Team Skills ───────────────────────────────────────────────────────────
    # Domain: team | Purpose: parallel fan-out/fan-in coordination
    # Team nodes contain member_skill_chaining — each member has own chain
    # Fan-out: all members execute in parallel
    # Fan-in: aggregator collects all member results
    # ACES parallel: $WFLAND barrier synchronization primitive

    TEAM_ENRICH            = "skills/team/enrich"
    # Enrichment team — three members, parallel execution, governed aggregation.
    # POC role: Step 3 — the Team node in the main graph.
    # member_skill_chaining:
    #     researcher:  [SEARCH_TAVILY]
    #     synthesizer: [TEXT_SUMMARIZE]
    #     formatter:   [TEXT_TRANSFORM]
    # Aggregator: merges all three member outputs into EnrichmentResult.
    # ACES parallel: parallel task steps with $WFLAND barrier at aggregation.

    TEAM_VALIDATE_PARALLEL = "skills/team/validate_parallel"
    # Parallel validation — multiple validators on same input simultaneously.
    # Phase 2 — defined now, implemented later.

    TEAM_RESEARCH          = "skills/team/research"
    # Research team — search + synthesize + cite in parallel.
    # Phase 2 — defined now, implemented later.
