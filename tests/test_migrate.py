import math
import os
import os.path
import pkgutil
import sys

import pytest

from kirameki import migrate


class TestLoader:
    def test_get_root_path(self):
        loader = migrate.Loader(__name__)
        assert loader.root_path == os.path.dirname(__file__)

    @pytest.mark.casedirs("migrate/test_loader/test_get_root_path")
    def test_get_root_path_unloaded(self, casedirs, monkeypatch):
        (d,) = casedirs
        monkeypatch.syspath_prepend(d)
        loader = migrate.Loader("_unloaded")
        assert loader.root_path == d

    def test_get_root_path_unloaded_main(self, monkeypatch):
        monkeypatch.delitem(sys.modules, "__main__")
        loader = migrate.Loader("__main__")
        assert loader.root_path == os.getcwd()

    def test_get_root_path_no_get_filename(self, monkeypatch):
        loader = pkgutil.find_loader(__name__)
        # NOTE(auri): this is probably unnecesary cause
        # pytest importer doesn't have get_filename but
        # we get rid of it just in case that changes
        monkeypatch.delattr(loader, "get_filename", raising=False)
        monkeypatch.delitem(sys.modules, __name__)

        with pytest.raises(RuntimeError, match="root_path"):
            migrate.Loader(__name__)


def test_simple_planner():
    migrations = [1, 2, 3, 4, 5, 6, 7, 8]
    planner = migrate.SimplePlanner(migrations)

    with pytest.raises(migrate.UnknownMigrationError, match="42"):
        planner.plan([1, 2, 3, 42], 8)

    with pytest.raises(migrate.StateHoleError, match="2"):
        planner.plan([1, 3, 4, 5], 8)

    state = migrations
    assert (
        planner.plan(state, 8)
        == planner.plan(state, math.inf)
        == ([], migrate.PlanDirection.UNCHANGED, 8, 8)
    )

    state = migrations[:-2]
    assert (
        planner.plan(state, 8)
        == planner.plan(state, math.inf)
        == (migrations[-2:], migrate.PlanDirection.FORWARD, 6, 8)
    )
    assert planner.plan(state, 7) == ([7], migrate.PlanDirection.FORWARD, 6, 7)

    state = migrations
    assert (
        planner.plan(state, -1)
        # TODO(auri): clamp target version to min avail version
        # == planner.plan(state, 0)
        == planner.plan(state, -math.inf)
        == (list(reversed(migrations)), migrate.PlanDirection.BACKWARD, 8, -1)
    )
    assert planner.plan(state, 4) == (
        list(reversed(migrations[-4:])),
        migrate.PlanDirection.BACKWARD,
        8,
        4,
    )
