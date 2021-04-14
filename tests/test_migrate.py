import hashlib
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


class TestMigrator:
    def test_plan_backwards(self, dummy_m):
        with pytest.raises(migrate.PlanningError, match="unapplied"):
            next(
                dummy_m._plan_backwards(
                    self._create_state(dummy_m, 0), 1618387500
                )
            )
        with pytest.raises(migrate.PlanningError, match="downable"):
            list(
                dummy_m._plan_backwards(
                    self._create_state(dummy_m, 0, 1, 2, 3), -1
                )
            )

    def test_get_current_version(self, dummy_m):
        with pytest.raises(migrate.StateIntegrityError, match="unknown"):
            dummy_m._get_current_version(
                [*self._create_state(dummy_m, 0), (1, "1")]
            )
        with pytest.raises(migrate.StateIntegrityError, match="hole"):
            dummy_m._get_current_version(self._create_state(dummy_m, 0, 2))
        with pytest.raises(migrate.StateIntegrityError, match="checksum"):
            dummy_m._get_current_version(
                [*self._create_state(dummy_m, 0), (1618387495, "xxx")]
            )
        assert dummy_m._get_current_version([]) == -1
        assert (
            dummy_m._get_current_version(self._create_state(dummy_m, 0, 1, 2))
            == 1618387500
        )

    def _create_state(self, migrator, *indices):
        return [
            (m.version, m.sha256)
            for m in migrator.migrations.values()
            for i in indices
            if m.version == migrator._versions[i]
        ]

    @pytest.fixture
    def dummy_m(self):
        migrations = [
            (
                1618387490,
                "CREATE TABLE dummy (id serial PRIMARY KEY)",
                "DROP TABLE dummy",
            ),
            (
                1618387495,
                "ALTER TABLE foo ADD COLUMN foo text",
                "ALTER TABLE foo DROP COLUMN name",
            ),
            (
                1618387500,
                "ALTER TABLE foo ADD COLUMN bar timestamp",
                None,
            ),
            (
                1618387505,
                "ALTER TABLE foo ADD COLUMN bar integer",
                "ALTER TABLE foo DROP COLUMN bar",
            ),
        ]
        return migrate.Migrator(
            None,
            [
                migrate.SQLMigration(
                    version=version,
                    up_sql=up_sql,
                    sha256=hashlib.sha256(up_sql.encode("utf-8")).hexdigest(),
                    down_sql=down_sql,
                )
                for version, up_sql, down_sql in migrations
            ],
        )

    @pytest.fixture
    def m(self, dummy_m, conn):
        dummy_m.conn = conn
        return dummy_m
