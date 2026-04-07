"""
registry/db.py

PostgreSQL connection management — three-node HA topology.

Architecture Standard: Mind Over Metadata LLC — Peter Heller
    Explicit naming: DatabaseRegistry NOT DbRegistry, DatabaseConfig NOT DbConfig
    Self-documenting: the name describes what the thing IS.
    See: ACES-Naming-Convention-Standard.md

Three-node topology (SRP — one role per node):
    Primary   (TheBeast)    — writes only, all registry mutations
    Replica 1 (TheBeast)    — Task Dispatcher routing reads, low latency
    Replica 2 (MiniBeast)   — analytics, DuckDB, pgvector HNSW semantic search

Security:
    Credentials via environment variables or Docker secrets — never hardcoded.
    All connections over Tailscale WireGuard mesh — encrypted in transit.
    Replicas reject direct application writes — WAL is the sole write path.
    ssl_mode defaults to REQUIRE — plaintext connections rejected.

D4 Principle:
    WAL is sole write path.
    Primary receives all writes.
    Replicas receive all reads of their designated type.
    No application writes to replicas. Ever.
"""

import os
from enum import Enum
from typing import AsyncGenerator

import asyncpg
from pydantic import BaseModel, Field, computed_field, field_validator


# ── Database Role ─────────────────────────────────────────────────────────────

class DatabaseRole(str, Enum):
    """
    Which node handles which workload.
    D4: named CHECK constraint on connection_config.role column.
    Explicit naming: DatabaseRole NOT DbRole.
    """
    PRIMARY    = "primary"
    # Read + Write. All registry mutations route here.
    # Node: TheBeast — RTX 5090 × 2, 96GB VRAM.
    # PostgreSQL port: 5432 on Tailscale mesh.

    REPLICA_1  = "replica_1"
    # Task Dispatcher routing reads — low latency required.
    # WAL-replicated from PRIMARY. Rejects all writes.
    # Node: TheBeast — same machine as PRIMARY, different PostgreSQL instance.
    # PostgreSQL port: 5433 on Tailscale mesh.

    REPLICA_2  = "replica_2"
    # Analytics + pg_duckdb + pgvector HNSW semantic search.
    # WAL-replicated from PRIMARY. Rejects all writes.
    # Node: MiniBeast — RTX 4090 × 2, 48GB VRAM.
    # PostgreSQL port: 5432 on Tailscale mesh.


class SslMode(str, Enum):
    DISABLE      = "disable"
    REQUIRE      = "require"
    VERIFY_CA    = "verify-ca"
    VERIFY_FULL  = "verify-full"


# ── Database Configuration ────────────────────────────────────────────────────

class DatabaseConfig(BaseModel):
    """
    Connection parameters for one PostgreSQL node.
    Explicit naming: DatabaseConfig NOT DbConfig.

    Instantiate via DatabaseRegistry — do not construct directly.
    Passwords loaded from environment variables — never passed as literals.
    """
    role:                   DatabaseRole
    host:                   str
    # Tailscale hostname — e.g. "thebeast.tail1234.ts.net"
    # Never an IP address — Tailscale DNS is the stable identifier.

    port:                   int           = 5432
    database:               str           = "aces_registry"
    user:                   str           = "aces"
    password:               str           = ""
    # "" = loaded from environment via field_validator below.
    # ACES_DB_PRIMARY_PASSWORD, ACES_DB_REPLICA_1_PASSWORD, ACES_DB_REPLICA_2_PASSWORD

    ssl_mode:               SslMode       = SslMode.REQUIRE
    pool_min_connections:   int           = 2
    pool_max_connections:   int           = 10
    connect_timeout_sec:    int           = 5
    command_timeout_sec:    int           = 30
    statement_cache_size:   int           = 100
    # asyncpg prepared statement cache — reduces parse overhead on repeat queries.

    @computed_field
    @property
    def dsn(self) -> str:
        """Full DSN with password — for asyncpg connection."""
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
            f"?sslmode={self.ssl_mode.value}"
        )

    @computed_field
    @property
    def dsn_safe(self) -> str:
        """DSN with password redacted — for logging. Never log dsn directly."""
        return (
            f"postgresql://{self.user}:***"
            f"@{self.host}:{self.port}/{self.database}"
            f"?sslmode={self.ssl_mode.value}"
        )

    @field_validator("password", mode="before")
    @classmethod
    def load_from_environment(cls, value: str, info) -> str:
        """
        Load password from environment variable if not provided directly.
        Environment variable names by role:
            PRIMARY   → ACES_DB_PRIMARY_PASSWORD
            REPLICA_1 → ACES_DB_REPLICA_1_PASSWORD
            REPLICA_2 → ACES_DB_REPLICA_2_PASSWORD
        """
        if value:
            return value
        role = info.data.get("role")
        env_map = {
            DatabaseRole.PRIMARY:   "ACES_DB_PRIMARY_PASSWORD",
            DatabaseRole.REPLICA_1: "ACES_DB_REPLICA_1_PASSWORD",
            DatabaseRole.REPLICA_2: "ACES_DB_REPLICA_2_PASSWORD",
        }
        env_key = env_map.get(role, "ACES_DB_PASSWORD")
        return os.environ.get(env_key, "")


# ── Database Registry ─────────────────────────────────────────────────────────

class DatabaseRegistry:
    """
    Manages connection pools for all three PostgreSQL nodes.
    Explicit naming: DatabaseRegistry NOT DbRegistry.

    Usage:
        registry = await DatabaseRegistry.create()
        async with registry.write() as conn:
            await conn.execute(INSERT_SQL, ...)
        async with registry.read_routing() as conn:
            row = await conn.fetchrow(ROUTING_QUERY, ...)
        async with registry.read_analytics() as conn:
            rows = await conn.fetch(ANALYTICS_QUERY, ...)
        await registry.close()

    D4 Principle:
        write()           → PRIMARY only — all mutations
        read_routing()    → REPLICA_1 only — task dispatcher queries
        read_analytics()  → REPLICA_2 only — analytics + pgvector
        No method routes writes to replicas. WAL is sole write path.
    """

    def __init__(
        self,
        primary:   DatabaseConfig,
        replica_1: DatabaseConfig,
        replica_2: DatabaseConfig,
    ) -> None:
        self._configs = {
            DatabaseRole.PRIMARY:   primary,
            DatabaseRole.REPLICA_1: replica_1,
            DatabaseRole.REPLICA_2: replica_2,
        }
        self._pools: dict[DatabaseRole, asyncpg.Pool] = {}

    @classmethod
    async def create(
        cls,
        primary_host:   str = "",
        replica_1_host: str = "",
        replica_2_host: str = "",
    ) -> "DatabaseRegistry":
        """
        Factory — creates and initializes all three connection pools.
        Hosts default to Tailscale hostnames from environment if not provided.
        """
        primary_host   = primary_host   or os.environ.get("ACES_PRIMARY_HOST",   "thebeast.tail1234.ts.net")
        replica_1_host = replica_1_host or os.environ.get("ACES_REPLICA_1_HOST", "thebeast.tail1234.ts.net")
        replica_2_host = replica_2_host or os.environ.get("ACES_REPLICA_2_HOST", "minibeast.tail1234.ts.net")

        registry = cls(
            primary=DatabaseConfig(
                role=DatabaseRole.PRIMARY,
                host=primary_host,
                port=5432,
            ),
            replica_1=DatabaseConfig(
                role=DatabaseRole.REPLICA_1,
                host=replica_1_host,
                port=5433,
            ),
            replica_2=DatabaseConfig(
                role=DatabaseRole.REPLICA_2,
                host=replica_2_host,
                port=5432,
            ),
        )
        await registry._initialize_pools()
        return registry

    async def _initialize_pools(self) -> None:
        """Create asyncpg connection pools for all three nodes."""
        for role, config in self._configs.items():
            self._pools[role] = await asyncpg.create_pool(
                dsn=config.dsn,
                min_size=config.pool_min_connections,
                max_size=config.pool_max_connections,
                timeout=config.connect_timeout_sec,
                command_timeout=config.command_timeout_sec,
                statement_cache_size=config.statement_cache_size,
            )

    def write(self) -> "asyncpg.pool.PoolConnectionProxy":
        """
        Acquire a PRIMARY connection for writes.
        All registry mutations must use this method.
        D4: WAL is sole write path — only PRIMARY accepts writes.
        """
        return self._pools[DatabaseRole.PRIMARY].acquire()

    def read_routing(self) -> "asyncpg.pool.PoolConnectionProxy":
        """
        Acquire a REPLICA_1 connection for task dispatcher routing reads.
        Low-latency reads — same machine as PRIMARY.
        """
        return self._pools[DatabaseRole.REPLICA_1].acquire()

    def read_analytics(self) -> "asyncpg.pool.PoolConnectionProxy":
        """
        Acquire a REPLICA_2 connection for analytics + pgvector queries.
        Heavy reads — separate machine (MiniBeast) — does not compete with PRIMARY.
        """
        return self._pools[DatabaseRole.REPLICA_2].acquire()

    async def health_check(self) -> dict[str, bool]:
        """
        Verify all three nodes are reachable and responding.
        Returns dict[role_name, is_healthy].
        Used by checkpoint-agent and SkillDeltaScanner startup.
        """
        results = {}
        for role, pool in self._pools.items():
            try:
                async with pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                results[role.value] = True
            except Exception:
                results[role.value] = False
        return results

    async def close(self) -> None:
        """Gracefully close all connection pools."""
        for pool in self._pools.values():
            await pool.close()

    def write_dsn(self) -> str:
        """Return PRIMARY DSN — for LangGraph PostgresSaver checkpointer."""
        return self._configs[DatabaseRole.PRIMARY].dsn

    def write_dsn_safe(self) -> str:
        """Return PRIMARY DSN with redacted password — for logging."""
        return self._configs[DatabaseRole.PRIMARY].dsn_safe


# ── DDL — Schema Definitions ──────────────────────────────────────────────────

SCHEMA_DDL = """
-- ═══════════════════════════════════════════════════════════════════════════
-- ACES Registry Schema
-- D4 Temporal Registry Pattern — Mind Over Metadata LLC — Peter Heller
-- ═══════════════════════════════════════════════════════════════════════════

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "vector";          -- pgvector for HNSW search

-- ── Skill Registry ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS skill_record (
    -- Business key
    fqsn                TEXT        NOT NULL,
    -- Version identity
    version             TEXT        NOT NULL,
    fqsn_hash           TEXT        NOT NULL,
    previous_hash       TEXT        NOT NULL DEFAULT '',
    -- Content
    system_md           TEXT        NOT NULL,
    user_md             TEXT        NOT NULL DEFAULT '',
    tool_binding        TEXT        NOT NULL DEFAULT '',
    diff                JSONB       NOT NULL DEFAULT '{}',
    -- Allen Interval
    valid_from          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to            TIMESTAMPTZ NOT NULL DEFAULT '9999-12-31 23:59:59.999999+00',
    -- Provenance
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deprecated_at       TIMESTAMPTZ NOT NULL DEFAULT '9999-12-31 23:59:59.999999+00',
    -- D4 named CHECK constraints — governance as code
    CONSTRAINT skill_record_pk          PRIMARY KEY (fqsn, version),
    CONSTRAINT skill_record_hash_notnull CHECK (fqsn_hash <> ''),
    CONSTRAINT skill_record_system_md   CHECK (system_md <> '' OR tool_binding <> ''),
    CONSTRAINT skill_record_valid_range CHECK (valid_from <= valid_to),
    CONSTRAINT skill_record_one_current UNIQUE (fqsn, valid_to)
    -- Partial unique: only one current version per skill (valid_to = open-ended)
);

CREATE INDEX IF NOT EXISTS idx_skill_record_current
    ON skill_record (fqsn)
    WHERE valid_to = '9999-12-31 23:59:59.999999+00';

-- ── Task Registry ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS task_record (
    fqsn                TEXT        NOT NULL,
    version             TEXT        NOT NULL,
    fqsn_hash           TEXT        NOT NULL,
    previous_hash       TEXT        NOT NULL DEFAULT '',
    description         TEXT        NOT NULL,
    operating_mode      TEXT        NOT NULL DEFAULT 'maas',
    skill_chaining      JSONB       NOT NULL,
    definition          JSONB       NOT NULL DEFAULT '{}',
    diff                JSONB       NOT NULL DEFAULT '{}',
    valid_from          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to            TIMESTAMPTZ NOT NULL DEFAULT '9999-12-31 23:59:59.999999+00',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deprecated_at       TIMESTAMPTZ NOT NULL DEFAULT '9999-12-31 23:59:59.999999+00',
    CONSTRAINT task_record_pk           PRIMARY KEY (fqsn, version),
    CONSTRAINT task_record_hash_notnull CHECK (fqsn_hash <> ''),
    CONSTRAINT task_record_description  CHECK (description <> ''),
    CONSTRAINT task_record_valid_range  CHECK (valid_from <= valid_to),
    CONSTRAINT task_record_mode         CHECK (operating_mode IN ('cloud','maas','hybrid')),
    CONSTRAINT task_record_one_current  UNIQUE (fqsn, valid_to)
);

-- ── Registry Events — append-only audit log ───────────────────────────────
CREATE TABLE IF NOT EXISTS registry_event (
    event_id            UUID        NOT NULL DEFAULT uuid_generate_v4(),
    event_type          TEXT        NOT NULL,
    event_source        TEXT        NOT NULL,
    fqsn                TEXT        NOT NULL,
    fqsn_hash           TEXT        NOT NULL DEFAULT '',
    previous_hash       TEXT        NOT NULL DEFAULT '',
    payload             JSONB       NOT NULL DEFAULT '{}',
    fired_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_from          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to            TIMESTAMPTZ NOT NULL DEFAULT '9999-12-31 23:59:59.999999+00',
    CONSTRAINT registry_event_pk    PRIMARY KEY (event_id),
    CONSTRAINT registry_event_type  CHECK (event_type <> ''),
    CONSTRAINT registry_event_fqsn  CHECK (fqsn <> '')
    -- No UPDATE, no DELETE — this table is append-only.
    -- Enforce via PostgreSQL row-level security in production.
);

CREATE INDEX IF NOT EXISTS idx_registry_event_fqsn
    ON registry_event (fqsn, fired_at DESC);

-- ── Workspace State — persisted audit trail ───────────────────────────────
CREATE TABLE IF NOT EXISTS workspace_state (
    task_id             TEXT        NOT NULL,
    task_fqsn           TEXT        NOT NULL,
    task_version        TEXT        NOT NULL,
    session_id          UUID        NOT NULL DEFAULT uuid_generate_v4(),
    entries             JSONB       NOT NULL DEFAULT '[]',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    status              TEXT        NOT NULL DEFAULT 'running',
    CONSTRAINT workspace_state_pk   PRIMARY KEY (session_id),
    CONSTRAINT workspace_status     CHECK (status IN ('running','completed','failed'))
);
"""
