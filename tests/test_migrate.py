import hashlib
import os
import os.path
import pkgutil
import sys
from contextlib import closing

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


class TestSQLLoader:
    @pytest.mark.casedirs("migrate/test_sql_loader/test_valid")
    def test_valid(self, casedirs, monkeypatch):
        (d,) = casedirs
        monkeypatch.syspath_prepend(d)
        loader = migrate.SQLLoader("test_module")
        assert sorted(loader.load_all(), key=lambda m: m.version) == [
            migrate.SQLMigration(
                version=1618469624,
                up_sql="SELECT 1;\n",
                sha256="b4e0497804e46e0a0b0b8c31975b062152d551bac49c3c2e80932567b4085dcd",
            ),
            migrate.SQLMigration(
                version=1618469656,
                up_sql="SELECT 2;\n",
                sha256="a41109d24069b4822ddc5f367b25d484dc7e839bff338ce7a3e5da641caacda0",
                down_sql="SELECT 3;\n",
            ),
        ]


class TestMigrator:
    def test_migrate(self, m, tmpdb_conn):
        def _dummy_columns():
            with tmpdb_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE
                        table_schema = 'public'
                        AND table_name = 'dummy'
                    ORDER BY ordinal_position
                    ASC
                    """
                )
                return [n for (n,) in cur]

        m.up(1618387490)
        assert _dummy_columns() == ["id"]
        m.down()
        assert _dummy_columns() == []
        m.up()
        assert _dummy_columns() == ["id", "foo", "bar", "baz", "qux"]
        m.down(1618387500)
        assert _dummy_columns() == ["id", "foo", "bar"]
        with pytest.raises(migrate.PlanningError, match="downable"):
            m.down()

        # TODO(auri): test isolation_level, dry_run, progress_callback
        # and partial failures (i.e. "are we running in a transaction?")

    def test_plan_backwards(self, dummy_m):
        with pytest.raises(migrate.PlanningError, match="unapplied"):
            next(
                dummy_m._plan_backwards(
                    self._create_state(dummy_m, 0), 1618387500, False
                )
            )
        with pytest.raises(migrate.PlanningError, match="downable"):
            list(
                dummy_m._plan_backwards(
                    self._create_state(dummy_m, 0, 1, 2, 3), -1, False
                )
            )

    def test_get_current_version(self, dummy_m):
        # TODO: test force=True
        with pytest.raises(migrate.StateIntegrityError, match="unknown"):
            dummy_m._get_current_version(
                [*self._create_state(dummy_m, 0), (1, "1")], False
            )
        with pytest.raises(migrate.StateIntegrityError, match="hole"):
            dummy_m._get_current_version(
                self._create_state(dummy_m, 0, 2), False
            )
        with pytest.raises(migrate.StateIntegrityError, match="checksum"):
            dummy_m._get_current_version(
                [*self._create_state(dummy_m, 0), (1618387495, "xxx")], False
            )
        assert dummy_m._get_current_version([], False) == -1
        assert (
            dummy_m._get_current_version(
                self._create_state(dummy_m, 0, 1, 2), False
            )
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
                "CREATE TABLE public.dummy (id serial PRIMARY KEY)",
                "DROP TABLE public.dummy",
            ),
            (
                1618387495,
                "ALTER TABLE public.dummy ADD COLUMN foo text",
                "ALTER TABLE public.dummy DROP COLUMN name",
            ),
            (
                1618387500,
                "ALTER TABLE public.dummy ADD COLUMN bar timestamp",
                None,
            ),
            (
                1618387505,
                "ALTER TABLE public.dummy ADD COLUMN baz integer",
                "ALTER TABLE public.dummy DROP COLUMN baz",
            ),
            (
                1618387510,
                "ALTER TABLE public.dummy ADD COLUMN qux integer",
                "ALTER TABLE public.dummy DROP COLUMN qux",
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
    def m(self, dummy_m, tmpdb):
        with closing(tmpdb.connect()) as conn:
            dummy_m.conn = conn
            yield dummy_m
