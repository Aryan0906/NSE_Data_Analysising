"""
tests/test_stock_pipeline_dag.py
==================================
Unit tests for backend/dags/stock_pipeline_dag.py — Sprint 4.

Tests verify:
  1. The DAG object has the expected id / schedule.
  2. Exactly 8 tasks exist.
  3. Every expected task_id is present.
  4. The dependency graph is correct (fan-out / fan-in wiring).
  5. All tasks are PythonOperator instances.
  6. DAG has catchup=False and max_active_runs=1.

No live Airflow installation, database, or network connection is required.
Both Airflow and the pipeline helpers are fully mocked before the DAG module
is loaded.

Run with:
    pytest tests/test_stock_pipeline_dag.py -v
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import types
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch


# ─────────────────────────────────────────────────────────────────────────────
# Build a minimal in-process Airflow stub so the DAG module can be imported
# without installing apache-airflow.
# ─────────────────────────────────────────────────────────────────────────────

_DAG_CONTEXT: list = []  # global stack mirroring Airflow's DagContext


def _get_current_dag():
    return _DAG_CONTEXT[-1] if _DAG_CONTEXT else None


class _Task:
    """Minimal task that tracks task_id + dependency lists.

    Supports both directions of the Airflow bitshift API:
      task >> other        — __rshift__
      task << other        — __lshift__
      [task, task] >> task — __rlshift__ on the RIGHT operand  (key fix)

    Tasks self-register into the currently-active DAG context, mirroring
    how real Airflow works when tasks are constructed inside `with DAG(...):`.
    """

    def __init__(self, task_id: str, python_callable=None, op_kwargs=None, dag=None, **kwargs):
        self.task_id = task_id
        self.python_callable = python_callable
        self.op_kwargs = op_kwargs or {}
        # Prefer explicit dag arg; fall back to context stack
        active_dag = dag if dag is not None else _get_current_dag()
        self._dag = active_dag
        self.upstream_list: list = []
        self.downstream_list: list = []
        if active_dag is not None:
            active_dag.task_dict[task_id] = self

    # ── dependency helpers ──────────────────────────────────────────────────

    def set_upstream(self, other) -> None:
        sources = list(other) if isinstance(other, (list, _TaskList)) else [other]
        for t in sources:
            if t not in self.upstream_list:
                self.upstream_list.append(t)
            if self not in t.downstream_list:
                t.downstream_list.append(self)

    def set_downstream(self, other) -> None:
        targets = list(other) if isinstance(other, (list, _TaskList)) else [other]
        for t in targets:
            t.set_upstream(self)

    # ── bitshift operators (left operand = self) ────────────────────────────

    def __rshift__(self, other):
        """task >> other  or  task >> [a, b]"""
        if isinstance(other, (list, _TaskList)):
            tl = _TaskList(other)
            for t in tl:
                self.set_downstream(t)
            return tl
        else:
            self.set_downstream(other)
            return other

    def __lshift__(self, other):
        """task << other  or  task << [a, b]"""
        sources = list(other) if isinstance(other, (list, _TaskList)) else [other]
        for src in sources:
            self.set_upstream(src)
        return other

    # ── reflected bitshift operators (left operand = list/other) ───────────

    def __rlshift__(self, other):
        """Handles  [a, b] >> self  (Python tries list.__rshift__ first, then our __rlshift__)."""
        sources = list(other) if isinstance(other, (list, _TaskList)) else [other]
        for src in sources:
            self.set_upstream(src)
        return self

    def __rrshift__(self, other):
        """`[a, b] >> self` — a and b become upstream of self."""
        sources = list(other) if isinstance(other, (list, _TaskList)) else [other]
        for src in sources:
            self.set_upstream(src)
        return self


class _TaskList:
    """Wraps a list of _Task objects so [a,b] >> c works even when
    the left side is already a _TaskList (e.g. chained dependencies)."""

    def __init__(self, tasks):
        self._tasks = list(tasks)

    def __iter__(self):
        return iter(self._tasks)

    def __rshift__(self, other):
        """_TaskList >> other"""
        if isinstance(other, (list, _TaskList)):
            tl = _TaskList(other)
            for t in tl:
                for src in self._tasks:
                    src.set_downstream(t)
            return tl
        else:
            for src in self._tasks:
                src.set_downstream(other)
            return other

    def __lshift__(self, other):
        """_TaskList << other"""
        sources = list(other) if isinstance(other, (list, _TaskList)) else [other]
        for target in self._tasks:
            for src in sources:
                target.set_upstream(src)
        return other

    def __rlshift__(self, other):
        """Handles  [a, b] >> _TaskList"""
        sources = list(other) if isinstance(other, (list, _TaskList)) else [other]
        for target in self._tasks:
            for src in sources:
                target.set_upstream(src)
        return self


class _DAG:
    """Minimal DAG that records tasks and exposes Airflow-like attributes."""
    def __init__(self, dag_id: str, *, schedule_interval=None, schedule=None,
                 default_args=None, catchup=True, max_active_runs=16, tags=None,
                 is_paused_upon_creation=None, **kwargs):
        self.dag_id = dag_id
        # Support both schedule_interval (old) and schedule (new) kwarg
        self.schedule_interval = schedule if schedule is not None else schedule_interval
        self.default_args = default_args or {}
        self.catchup = catchup
        self.max_active_runs = max_active_runs
        self.tags = tags or []
        self.is_paused_upon_creation = is_paused_upon_creation
        self.task_dict: dict = {}

    @property
    def tasks(self):
        return list(self.task_dict.values())

    @property
    def task_ids(self):
        return list(self.task_dict.keys())

    def get_task(self, task_id: str):
        if task_id not in self.task_dict:
            raise KeyError(f"Task {task_id!r} not found in DAG")
        return self.task_dict[task_id]

    def __enter__(self):
        _DAG_CONTEXT.append(self)
        return self

    def __exit__(self, *args):
        _DAG_CONTEXT.pop()


# Install the Airflow stub modules ────────────────────────────────────────────

def _make_mod(name: str) -> types.ModuleType:
    m = types.ModuleType(name)
    return m


_airflow = _make_mod("airflow")
_airflow.DAG = _DAG  # type: ignore[attr-defined]
sys.modules.setdefault("airflow", _airflow)

_models = _make_mod("airflow.models")
_models.DAG = _DAG  # type: ignore[attr-defined]
sys.modules.setdefault("airflow.models", _models)

_python_op = _make_mod("airflow.operators.python")
_python_op.PythonOperator = _Task  # type: ignore[attr-defined]
sys.modules.setdefault("airflow.operators.python", _python_op)

_utils_dates = _make_mod("airflow.utils.dates")
_utils_dates.days_ago = lambda n: datetime.utcnow() - timedelta(days=n)  # type: ignore[attr-defined]
sys.modules.setdefault("airflow.utils.dates", _utils_dates)

_utils = _make_mod("airflow.utils")
sys.modules.setdefault("airflow.utils", _utils)

# ─────────────────────────────────────────────────────────────────────────────
# Stub the pipeline modules (only leaf modules — backend/ stays a real package)
# ─────────────────────────────────────────────────────────────────────────────

def _pipeline_stub(**attrs) -> types.ModuleType:
    m = types.ModuleType("_stub")
    m.__dict__.update(attrs)
    return m


_DB_INIT   = _pipeline_stub(run=MagicMock(return_value=None))
_EXTRACT   = _pipeline_stub(run=MagicMock(return_value=0))
_NEWS_INGEST = _pipeline_stub(run=MagicMock(return_value=0))
_LOAD      = _pipeline_stub(run=MagicMock(return_value=0))
_TRANSFORM = _pipeline_stub(
    run=MagicMock(return_value=0),
    transform_prices=MagicMock(return_value=0),
    transform_fundamentals=MagicMock(return_value=0),
    transform_news=MagicMock(return_value=0),
)

_fake_settings = MagicMock()
_fake_settings.nse_symbols = ["RELIANCE.NS", "TCS.NS"]
_SETTINGS_MOD = _pipeline_stub(settings=_fake_settings)

for _path, _stub in {
    "backend.pipeline.db_init":    _DB_INIT,
    "backend.pipeline.extract":    _EXTRACT,
    "backend.pipeline.news_ingest": _NEWS_INGEST,
    "backend.pipeline.load":       _LOAD,
    "backend.pipeline.transform":  _TRANSFORM,
    "backend.pipeline.settings":   _SETTINGS_MOD,
}.items():
    sys.modules.setdefault(_path, _stub)


# ─────────────────────────────────────────────────────────────────────────────
# Load the DAG module from disk (bypass package import machinery)
# ─────────────────────────────────────────────────────────────────────────────

_DAG_FILE = (
    pathlib.Path(__file__).parent.parent
    / "backend" / "dags" / "stock_pipeline_dag.py"
)

_spec = importlib.util.spec_from_file_location(
    "backend.dags.stock_pipeline_dag",
    str(_DAG_FILE),
)
dag_module = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["backend.dags.stock_pipeline_dag"] = dag_module
_spec.loader.exec_module(dag_module)  # type: ignore[union-attr]


# ─────────────────────────────────────────────────────────────────────────────
# Constants for tests
# ─────────────────────────────────────────────────────────────────────────────

EXPECTED_TASK_IDS: list[str] = [
    "db_init",
    "extract_prices",
    "news_ingest",
    "transform_prices",
    "transform_news",
    "load_gold",
    "create_views",
    "validate_schema",
]

EXPECTED_EDGES: list[tuple[str, str]] = [
    ("db_init", "extract_prices"),
    ("db_init", "news_ingest"),
    ("extract_prices", "transform_prices"),
    ("news_ingest", "transform_news"),
    ("transform_prices", "load_gold"),
    ("transform_news", "load_gold"),
    ("load_gold", "create_views"),
    ("create_views", "validate_schema"),
]


def _dag() -> _DAG:
    return dag_module.dag  # type: ignore[return-value]


def _task(task_id: str) -> _Task:
    return _dag().get_task(task_id)


def _upstream(task_id: str) -> set[str]:
    return {t.task_id for t in _task(task_id).upstream_list}


def _downstream(task_id: str) -> set[str]:
    return {t.task_id for t in _task(task_id).downstream_list}


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestDagMetadata:
    def test_dag_id(self):
        assert _dag().dag_id == "stock_pipeline"

    def test_schedule(self):
        assert _dag().schedule_interval == "30 13 * * 1-5"

    def test_catchup_disabled(self):
        assert _dag().catchup is False

    def test_max_active_runs(self):
        assert _dag().max_active_runs == 1

    def test_tags_sprint4(self):
        assert "sprint4" in _dag().tags

    def test_tags_nse(self):
        assert "nse" in _dag().tags


class TestTaskCount:
    def test_exactly_eight_tasks(self):
        actual   = sorted(t.task_id for t in _dag().tasks)
        expected = sorted(EXPECTED_TASK_IDS)
        assert actual == expected, f"Expected {expected}, got {actual}"


class TestTaskIds:
    def test_db_init(self):          assert "db_init" in _dag().task_ids
    def test_extract_prices(self):   assert "extract_prices" in _dag().task_ids
    def test_news_ingest(self):      assert "news_ingest" in _dag().task_ids
    def test_transform_prices(self): assert "transform_prices" in _dag().task_ids
    def test_transform_news(self):   assert "transform_news" in _dag().task_ids
    def test_load_gold(self):        assert "load_gold" in _dag().task_ids
    def test_create_views(self):     assert "create_views" in _dag().task_ids
    def test_validate_schema(self):  assert "validate_schema" in _dag().task_ids


class TestDependencyGraph:
    """
    Verify fan-out / fan-in:

        db_init ──► extract_prices ──► transform_prices ──►┐
                └─► news_ingest    ──► transform_news   ──►┼─► load_gold ──► create_views ──► validate_schema
    """

    def test_all_expected_edges(self):
        for up, down in EXPECTED_EDGES:
            assert down in _downstream(up), f"Missing edge: {up} >> {down}"

    def test_db_init_no_upstream(self):
        assert _upstream("db_init") == set()

    def test_db_init_fanout(self):
        assert _downstream("db_init") == {"extract_prices", "news_ingest"}

    def test_extract_prices_upstream(self):
        assert _upstream("extract_prices") == {"db_init"}

    def test_extract_prices_downstream(self):
        assert _downstream("extract_prices") == {"transform_prices"}

    def test_news_ingest_upstream(self):
        assert _upstream("news_ingest") == {"db_init"}

    def test_news_ingest_downstream(self):
        assert _downstream("news_ingest") == {"transform_news"}

    def test_transform_prices_upstream(self):
        assert _upstream("transform_prices") == {"extract_prices"}

    def test_transform_prices_downstream(self):
        assert _downstream("transform_prices") == {"load_gold"}

    def test_transform_news_upstream(self):
        assert _upstream("transform_news") == {"news_ingest"}

    def test_transform_news_downstream(self):
        assert _downstream("transform_news") == {"load_gold"}

    def test_load_gold_fanin(self):
        assert _upstream("load_gold") == {"transform_prices", "transform_news"}

    def test_load_gold_downstream(self):
        assert _downstream("load_gold") == {"create_views"}

    def test_create_views_upstream(self):
        assert _upstream("create_views") == {"load_gold"}

    def test_create_views_downstream(self):
        assert _downstream("create_views") == {"validate_schema"}

    def test_validate_schema_upstream(self):
        assert _upstream("validate_schema") == {"create_views"}

    def test_validate_schema_is_leaf(self):
        assert _downstream("validate_schema") == set()

    def test_prices_branch_isolated_from_news_transform(self):
        assert "transform_news" not in _downstream("extract_prices")

    def test_news_branch_isolated_from_prices_transform(self):
        assert "transform_prices" not in _downstream("news_ingest")


class TestTaskOperatorType:
    """All tasks must be _Task (our PythonOperator stub) instances."""

    def test_all_tasks_are_python_operator(self):
        for task_id in EXPECTED_TASK_IDS:
            t = _task(task_id)
            assert isinstance(t, _Task), (
                f"{task_id} is {type(t).__name__}, expected PythonOperator"
            )


class TestDefaultArgs:
    def test_owner(self):
        assert dag_module.DEFAULT_ARGS["owner"] == "nse-data-team"

    def test_retries(self):
        assert dag_module.DEFAULT_ARGS["retries"] == 2

    def test_email_on_failure(self):
        assert dag_module.DEFAULT_ARGS["email_on_failure"] is False

    def test_depends_on_past(self):
        assert dag_module.DEFAULT_ARGS["depends_on_past"] is False


class TestTaskCallables:
    """Smoke-test each callable with mocked pipeline modules."""

    def test_db_init_callable(self):
        _DB_INIT.run.reset_mock()
        dag_module._task_db_init()
        _DB_INIT.run.assert_called_once_with()

    def test_extract_prices_callable(self):
        _EXTRACT.run.reset_mock()
        _EXTRACT.run.return_value = 5
        result = dag_module._task_extract_prices(symbols=["X.NS"])
        _EXTRACT.run.assert_called_once_with(symbols=["X.NS"])
        assert result == 5

    def test_news_ingest_callable(self):
        _NEWS_INGEST.run.reset_mock()
        _NEWS_INGEST.run.return_value = 3
        result = dag_module._task_news_ingest(symbols=["X.NS"])
        _NEWS_INGEST.run.assert_called_once_with(symbols=["X.NS"])
        assert result == 3

    def test_transform_prices_returns_dict(self):
        with (
            patch.object(dag_module, "transform_prices", return_value=10) as mp,
            patch.object(dag_module, "transform_fundamentals", return_value=1) as mf,
        ):
            result = dag_module._task_transform_prices(symbols=["X.NS"])
        assert isinstance(result, dict)
        assert result["prices"] == 10
        assert result["fundamentals"] == 1

    def test_transform_news_accumulates_across_symbols(self):
        with patch.object(dag_module, "_transform_news_fn", return_value=7):
            result = dag_module._task_transform_news(symbols=["X.NS", "Y.NS"])
        assert result == 14  # 7 per symbol × 2

    def test_load_gold_callable(self):
        _LOAD.run.reset_mock()
        _LOAD.run.return_value = 4
        result = dag_module._task_load_gold(symbols=["X.NS"])
        _LOAD.run.assert_called_once_with(symbols=["X.NS"])
        assert result == 4
