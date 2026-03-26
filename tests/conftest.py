"""
tests/conftest.py
=================
Shared pytest fixtures for unit tests.

Since pydantic-settings validates required env vars at *import time*, we inject
dummy values before any pipeline module is imported.  No real database or
network connections are ever established by these tests.
"""
from __future__ import annotations

import os
import sys

# ── 1. Add the repo root to sys.path ────────────────────────────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# ── 2. Inject required env vars BEFORE any pipeline module is imported ───────
#    (pydantic-settings reads these at class-body evaluation time)
_REQUIRED_DUMMY_ENV = {
    "POSTGRES_HOST":              "localhost",
    "POSTGRES_DB":                "nse_test",
    "POSTGRES_USER":              "test_user",
    "POSTGRES_PASSWORD":          "test_pass",
    "POSTGRES_READONLY_PASSWORD": "test_ro_pass",
}
for _k, _v in _REQUIRED_DUMMY_ENV.items():
    os.environ.setdefault(_k, _v)

# ── 3. Stub heavy dependencies so module-level code doesn't open sockets ─────
#    (psycopg2.connect, redis.Redis, and chromadb.HttpClient are called at
#     import time inside transform.py / hf_client.py; we intercept them here.)
from unittest.mock import MagicMock

def _make_pg_stub():
    """Minimal psycopg2 stub — just enough so `import psycopg2` works."""
    mod = MagicMock(name="psycopg2")
    mod.connect.return_value = MagicMock(name="psycopg2.connection")
    mod.extras = MagicMock(name="psycopg2.extras")
    mod.extensions = MagicMock(name="psycopg2.extensions")
    return mod

_pg = _make_pg_stub()

for _name in ("psycopg2", "psycopg2.extras", "psycopg2.extensions"):
    sys.modules.setdefault(_name, _pg)

# Redis stub
_redis_mod = MagicMock(name="redis")
_redis_mod.Redis.return_value = MagicMock(name="redis.Redis_instance")
sys.modules.setdefault("redis", _redis_mod)

# ChromaDB stub (optional dependency)
_chroma_mod = MagicMock(name="chromadb")
_chroma_mod.HttpClient.return_value = MagicMock(name="chromadb.HttpClient_instance")
sys.modules.setdefault("chromadb", _chroma_mod)


