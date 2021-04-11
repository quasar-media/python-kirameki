import math
import os
import os.path
import pkgutil
import sys

import pytest

from kirameki import migrate


class TestPyLoader:
    @pytest.mark.casedirs("migrate/test_pyloader/validate_0")
    def test_valid_0(self, casedirs):
        (d,) = casedirs
        loader = migrate.PyLoader(
            "test_module", root_path=os.path.join(d, "test_module")
        )
        migrations = loader.load_all()
        assert not loader.errors
        assert not loader.warnings
        migrations = [
            (m.version, m.description, m.downable) for m in migrations
        ]
        assert migrations == [
            (1618113091, "initial", True),
            (1618113298, "one", True),
            (1618113302, "two", False),
            (1618113304, "three", False),
        ]

    def test_get_root_path(self):
        loader = migrate.PyLoader(__name__)
        assert loader.root_path == os.path.dirname(__file__)

    @pytest.mark.casedirs("migrate/test_pyloader/test_get_root_path")
    def test_get_root_path_unloaded(self, casedirs, monkeypatch):
        (d,) = casedirs
        monkeypatch.syspath_prepend(d)
        loader = migrate.PyLoader("_unloaded")
        assert loader.root_path == d

    def test_get_root_path_unloaded_main(self, monkeypatch):
        monkeypatch.delitem(sys.modules, "__main__")
        loader = migrate.PyLoader("__main__")
        assert loader.root_path == os.getcwd()

    def test_get_root_path_no_get_filename(self, monkeypatch):
        loader = pkgutil.find_loader(__name__)
        # NOTE(auri): this is probably unnecesary cause
        # pytest importer doesn't have get_filename but
        # we get rid of it just in case that changes
        monkeypatch.delattr(loader, "get_filename", raising=False)
        monkeypatch.delitem(sys.modules, __name__)

        with pytest.raises(RuntimeError, match="root_path"):
            migrate.PyLoader(__name__)


def test_simple_planner():
    migrations = [1, 2, 3, 4, 5, 6, 7, 8]
    planner = migrate.SimplePlanner(migrations)

    with pytest.raises(migrate.UnknownMigrationError, match="42"):
        planner.plan([1, 2, 3, 42])

    with pytest.raises(migrate.StateHoleError, match="2"):
        planner.plan([1, 3, 4, 5])

    state = migrations
    assert (
        planner.plan(state)
        == planner.plan(state, 8)
        == planner.plan(state, math.inf)
        == ([], migrate.PlanDirection.UNCHANGED, 8, 8)
    )

    state = migrations[:-2]
    assert (
        planner.plan(state)
        == planner.plan(state, 8)
        == planner.plan(state, math.inf)
        == (migrations[-2:], migrate.PlanDirection.FORWARD, 6, 8)
    )
    assert planner.plan(state, 7) == ([7], migrate.PlanDirection.FORWARD, 6, 7)

    state = migrations
    assert (
        planner.plan(state, -1)
        == planner.plan(state, 0)
        == planner.plan(state, -math.inf)
        == (list(reversed(migrations)), migrate.PlanDirection.BACKWARD, 8, 0)
    )
    assert planner.plan(state, 4) == (
        list(reversed(migrations[-4:])),
        migrate.PlanDirection.BACKWARD,
        8,
        4,
    )
