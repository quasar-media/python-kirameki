import argparse
import collections
import hashlib
import logging
import math
import os
import os.path
import pkgutil
import re
import shutil
import sys


class Migration:
    def __init__(self, version):
        self.version = version

    @property
    def downable(self):
        return self.down is not None

    def up(self, conn):
        raise NotImplementedError()

    down = None

    def __repr__(self):
        s = "<migration version={!r} uppable"
        if self.downable:
            s += " downable"
        s += ">"
        return s.format(self.version)


class Loader:
    migration_class = Migration

    def __init__(self, import_name, root_path=None):
        self.import_name = import_name
        self.root_path = root_path or self._get_root_path()
        self.errors = collections.OrderedDict()
        self.warnings = collections.OrderedDict()

    def load_all(self):
        raise NotImplementedError()

    def _report_error(self, file, msg):
        self._append_fault(self.errors, file, msg)

    def _report_warning(self, file, msg):
        self._append_fault(self.warnings, file, msg)

    def _append_fault(self, holder, file, msg):
        try:
            faults = holder[file]
        except KeyError:
            faults = holder[file] = []
        faults.append(msg)

    def _get_root_path(self):
        try:
            module = sys.modules[self.import_name]
            fp = module.__file__
        except (KeyError, AttributeError):
            loader = pkgutil.find_loader(self.import_name)
            if self.import_name == "__main__" or loader is None:
                return os.getcwd()
            try:
                fp = loader.get_filename(self.import_name)
            except AttributeError:
                raise RuntimeError(
                    "could not deduce root_path; please specify it explicitly"
                )
        return os.path.abspath(os.path.dirname(fp))


class SQLMigration(Migration):
    def __init__(self, *args, up_sql, sha256, down_sql=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.up_sql = up_sql
        self.sha256 = sha256
        self.down_sql = down_sql
        if down_sql is not None:
            self.down = self._down

    def up(self, conn):
        with conn.cursor() as cur:
            cur.execute(self.up_sql)

    def _down(self, conn):
        with conn.cursor() as cur:
            cur.execute(self.down_sql)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return (self.version, self.up_sql, self.sha256, self.down_sql) == (
            self.version,
            other.up_sql,
            self.sha256,
            self.down_sql,
        )


class SQLLoader(Loader):
    migration_class = SQLMigration

    _script = collections.namedtuple("_script", "file sha256 source")

    _name_re = re.compile(r"m_(?P<version>[\d_]+)_.+")

    def __init__(self, *args, migration_dir=None, encoding=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.migration_dir = migration_dir or "migrations"
        self.encoding = encoding or "utf-8"

    def load_all(self):
        parent = os.path.join(self.root_path, self.migration_dir)
        scripts = {}
        for basename in os.listdir(parent):
            name = os.path.join(parent, basename)
            if os.path.isdir(name):
                self._report_warning(name, "is a directory")
                continue
            info = self._parse_filename(name, basename)
            if not info:
                continue
            with open(name, "rb") as f:
                source = f.read()
                hash = hashlib.sha256(source).hexdigest()
                source = source.decode(self.encoding)
            version, is_down = info
            s = scripts.setdefault(version, [None, None])
            s[1 if is_down else 0] = self._script(name, hash, source)

        for version, (up, down) in scripts.items():
            if not up:
                self._report_error(
                    down.file, "has no accompanying up migration"
                )
                continue
            kwargs = dict(up_sql=up.source, sha256=up.sha256)
            if down:
                kwargs["down_sql"] = down.source
            yield self.migration_class(version, **kwargs)

    def _parse_filename(self, name, basename):
        rest, ext = os.path.splitext(basename)
        if ext != ".sql":
            self._report_warning(name, "is not a migration (need .sql suffix)")
            return None
        is_down = False
        if rest.endswith(".up"):
            rest = rest[:-3]
        elif rest.endswith(".down"):
            is_down = True
            rest = rest[:-5]
        if not rest.isidentifier():
            self._report_error(name, "is not a Python identifier")
            return None
        m = self._name_re.match(rest)
        if not m:
            self._report_error(name, "does not conform to name spec")
            return None
        version = m.group("version")
        try:
            version = int(version, 10)
        except ValueError:
            self._report(name, "version is not a valid integer")
            return None
        return version, is_down


class MigrationError(Exception):
    pass


class StateIntegrityError(MigrationError):
    pass


class PlanningError(MigrationError):
    pass


class Migrator:
    _log = logging.getLogger(__qualname__)

    def __init__(self, conn, migrations):
        if not migrations:
            raise ValueError(
                "cannot create a migrator out of empty migration set"
            )
        self.conn = conn
        self.migrations = collections.OrderedDict(
            [
                (m.version, m)
                for m in sorted(migrations, key=lambda m: m.version)
            ]
        )

        # XXX(auri): assuming the dict is sorted
        self._versions = list(self.migrations.keys())

    def up(self, target=None, **kwargs):
        if target is None:
            target = self._versions[-1]
        return self._migrate(
            self._plan_forwards, self._apply_forwards, target, **kwargs
        )

    def down(self, target=None, **kwargs):
        if target is None:
            target = -math.inf
        return self._migrate(
            self._plan_backwards, self._apply_backwards, target, **kwargs
        )

    def _migrate(
        self,
        plan,
        apply,
        target,
        isolation_level="default",
        dry_run=False,
        force=False,
        progress_callback=None,
    ):
        self.conn.set_session(autocommit=True)
        self._log.debug("creating history table if it doesn't exist yet")
        with self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS __kirameki_history__ (
                    version integer PRIMARY KEY,
                    sha256 character(64) NOT NULL,
                    applied_on timestamp
                        DEFAULT (now() at time zone 'utc')
                        NOT NULL
                )
                """
            )

        self.conn.set_session(
            autocommit=False, isolation_level=isolation_level
        )
        with self.conn.cursor() as cur:
            self._log.debug("acquiring access exclusive lock on history table")
            cur.execute(
                """
                LOCK TABLE __kirameki_history__
                IN ACCESS EXCLUSIVE MODE
                """
            )
            cur.execute(
                """
                SELECT version, sha256
                FROM __kirameki_history__
                ORDER BY version ASC
                """
            )
            state = list(cur)

        affected = [v for (v, _) in state]
        m = None
        try:
            for m in plan(state, target, force):
                apply(m)
                self._progress(progress_callback, m, True)
            if not dry_run:
                self.conn.commit()
        except Exception:
            if m is not None:
                self._progress(progress_callback, m, False)
            raise
        finally:
            self.conn.rollback()
        return affected

    def _progress(self, cb, *args):
        if cb is None:
            return
        try:
            cb(*args)
        except Exception:
            self._log.error("progress_callback raised", exc_info=True)

    def _apply_forwards(self, m):
        self._log.debug("up %r", m)
        m.up(self.conn)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO __kirameki_history__ (version, sha256)
                VALUES (%s, %s)
                """,
                (m.version, m.sha256),
            )

    def _apply_backwards(self, m):
        self._log.debug("down %r", m)
        m.down(self.conn)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM __kirameki_history__
                WHERE version = %s
                """,
                (m.version,),
            )

    def _plan_forwards(self, state, target, force):
        current = self._get_current_version(state, force)
        for v, m in self.migrations.items():
            if v > current and v <= target:
                yield m

    def _plan_backwards(self, state, target, force):
        current = self._get_current_version(state, force)
        if target > current:
            raise PlanningError("requested rollback to unapplied migration")
        for v, m in reversed(self.migrations.items()):
            if v <= current and v > target:
                if not m.downable:
                    raise PlanningError(
                        "requested rollback cannot proceed cause version {} "
                        "is not downable".format(v)
                    )
                yield m

    def _get_current_version(self, state, force):
        for their_ver, _ in state:
            if their_ver not in self.migrations and not force:
                raise StateIntegrityError(
                    "unknown migration: {}".format(their_ver)
                )
        for (their_ver, their_sha256), (our_ver, our_m) in zip(
            state, self.migrations.items()
        ):
            if their_ver != our_ver and not force:
                raise StateIntegrityError(
                    "hole in the state: {}".format(our_ver)
                )
            if their_sha256 != our_m.sha256 and not force:
                raise StateIntegrityError(
                    "checksum mismatch in "
                    "version {}: {!r} != {!r}".format(
                        our_ver, their_sha256, our_m.sha256
                    )
                )
        ver = -1
        if state:
            (ver, _) = state[-1]
        self._log.debug("current version = %s", ver)
        return ver


class _CLI:
    def __init__(self, m):
        self.m = m
        self.parser = self._create_parser()
        self.args = None

    def up_cmd(self):
        return self._migrate_cmd(self.m.up)

    def down_cmd(self):
        return self._migrate_cmd(self.m.down)

    def _migrate_cmd(self, action):
        def progress(m, success):
            if self.args.progress:
                self._printerr(
                    "{}: {}",
                    action.__name__ if success else "fail",
                    m.version,
                )

        try:
            action(
                self.args.target,
                isolation_level=self.args.isolation_level,
                dry_run=self.args.dry_run,
                force=self.args.force,
                progress_callback=progress,
            )
        except StateIntegrityError as e:
            self._printerr("database state integrity violation: {}", e)
            return 1
        except PlanningError as e:
            self._printerr("invalid target specified: {}", e)
            return 1
        # TODO(auri): consider nicer OperationalErrors?
        return 0

    def _create_parser(self):
        parser = argparse.ArgumentParser(
            prog="{} -m {}".format(self._get_python(), self.m.import_name),
            description="Manage migrations and database state.",
        )
        parser.add_argument(
            "-v",
            "--verbose",
            default=0,
            action="count",
            help="increase verbosity level (default: none)",
        )
        parser.add_argument(
            "--force",
            default=False,
            action="store_const",
            const=True,
            help="forcefully perform migration (default: no)",
        )
        parser.add_argument(
            "--progress",
            default=False,
            action="store_const",
            const=True,
            help="display progress (default: no)",
        )
        parser.add_argument(
            "-n",
            "--dry-run",
            default=False,
            action="store_const",
            const=True,
            help="do not commit changes (default: no)",
        )
        parser.add_argument(
            "--isolation-level",
            default="default",
            type=str,
            help="transaction isolation level (default: database default)",
        )

        subparsers = parser.add_subparsers(
            title="Commands", dest="COMMAND", required=True
        )

        up = subparsers.add_parser("up")
        up.add_argument(
            "target",
            metavar="TARGET",
            default=None,
            type=int,
            nargs="?",
            help="target version (default: latest)",
        )

        down = subparsers.add_parser("down")
        down.add_argument(
            "target",
            metavar="TARGET",
            default=None,
            type=int,
            help="target version",
        )

        return parser

    def _get_python(self):
        exe = sys.executable
        for python in ("python", "python3", os.path.basename(exe)):
            if os.path.realpath(shutil.which(python)) == os.path.realpath(exe):
                return python
        return exe

    def _printerr(self, msg, *args):
        print(msg.format(*args), file=sys.stderr)

    def __call__(self, args=None):
        self.args = self.parser.parse_args(args)
        logging.basicConfig(
            level=max(logging.DEBUG, logging.WARNING - self.args.verbose * 10)
        )
        self.m.load()
        sys.exit(getattr(self, self.args.COMMAND + "_cmd")())


class LoadFailure(RuntimeError):
    pass


class Migrate:
    migrator_class = Migrator

    _log = logging.getLogger(__qualname__)

    def __init__(self, import_name, connection_factory):
        self.import_name = import_name
        self.connection_factory = connection_factory

        self._migrations = None

    def load(self, loader=None):
        if self._migrations:
            return

        loader = loader or SQLLoader(self.import_name)
        self._migrations = list(loader.load_all())

        for file, msgs in loader.warnings.items():
            for msg in msgs:
                self._log.warning("%s: %s", file, msg)
        if loader.errors:
            for file, msgs in loader.errors.items():
                for msg in msgs:
                    self._log.error("%s: %s", file, msg)
            raise LoadFailure("load failed")

    def up(self, *args, **kwargs):
        return self._migrate("up", *args, **kwargs)

    def down(self, *args, **kwargs):
        return self._migrate("down", *args, **kwargs)

    def run_cli(self, args=None):
        cli = _CLI(self)
        cli(args)

    def _migrate(self, meth, *args, **kwargs):
        with self.connection_factory() as conn:
            migrator = Migrator(conn, self._migrations)
            return getattr(migrator, meth)(*args, **kwargs)
