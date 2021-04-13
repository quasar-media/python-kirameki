import argparse
import collections
import enum
import hashlib
import itertools
import logging
import os
import os.path
import pkgutil
import re
import shutil
import sys

from psycopg2 import errors


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
        self._fault_count = itertools.count()

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
        faults.append((next(self._fault_count), msg))

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


class PlanningError(Exception):
    pass


class UnknownMigrationError(PlanningError):
    pass


class StateHoleError(PlanningError):
    pass


class PlanDirection(enum.Enum):
    BACKWARD = -1
    UNCHANGED = 0
    FORWARD = 1


class Planner:
    def __init__(self, versions):
        self._versions = versions

    def plan(self, state, target):
        raise NotImplementedError()


class SimplePlanner(Planner):
    def plan(self, state, target):
        target = min(target, self._versions[-1])
        target = max(target, -1)

        current = self._get_current_version(state)
        plan = []
        direction = None
        if current == target:
            direction = PlanDirection.UNCHANGED
        elif target > current:
            direction = PlanDirection.FORWARD
            for v in self._versions:
                if v > current and v <= target:
                    plan.append(v)
        else:
            direction = PlanDirection.BACKWARD
            for v in reversed(self._versions):
                if v <= current and v > target:
                    plan.append(v)

        return plan, direction, current, target

    def _get_current_version(self, state):
        if not state:
            return -1

        version_set = set(self._versions)
        # First, we verify we know of every single version
        # contained within the state.
        for v in state:
            if v not in version_set:
                raise UnknownMigrationError(v)

        # Then, we verify that there are no holes within
        # the state.
        for their_ver, our_ver in zip(state, self._versions):
            if their_ver != our_ver:
                raise StateHoleError(our_ver)

        return state[-1]


class _CLI:
    def __init__(self, m, prog=None):
        self.m = m
        self.prog = prog
        self.parser = self._create_parser()
        self.args = None

    def up_cmd(self):
        # TODO(auri): pretty plan errors
        self.m.up(
            target=self.args.target,
            isolation_level=self.args.isolation_level,
            num_retries=self.args.num_retries,
            progress_callback=self._progress_callback,
        )
        return 0

    def down_cmd(self):
        self.m.down(
            target=self.args.target,
            isolation_level=self.args.isolation_level,
            num_retries=self.args.num_retries,
            progress_callback=self._progress_callback,
        )
        return 0

    def _progress_callback(self, version, state):
        if self.args.progress:
            self._printerr("{}: {}", version, "OK" if state else "FAIL")

    def _printerr(self, msg, *args):
        print(msg.format(*args), file=sys.stderr)

    def _create_parser(self):
        parser = argparse.ArgumentParser(
            prog=self.prog
            or "{} -m {}".format(self._get_python(), self.m.import_name),
            description="Manage migrations and database state.",
        )
        parser.add_argument(
            "--progress",
            default=False,
            action="store_const",
            const=True,
            help="display progress (default: no)",
        )
        parser.add_argument(
            "--isolation-level",
            default="default",
            type=str,
            help="transaction isolation level (default: database default)",
        )
        parser.add_argument(
            "--num-retries",
            default=0,
            type=int,
            help="number of times to retry serialization (default: 0)",
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
            metavar="target",
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

    def __call__(self, args=None):
        self.m.load()
        self.args = self.parser.parse_args(args)
        return getattr(self, self.args.COMMAND + "_cmd")()


class LoadFailed(RuntimeError):
    def __init__(self, msg, *, errors, warnings):
        super().__init__(msg, errors, warnings)
        self.msg = msg
        self.errors = errors
        self.warnings = warnings

    def __str__(self):
        # TODO(auri): format self.errors
        return self.msg


class Migrate:
    _log = logging.getLogger(__qualname__)

    def __init__(
        self,
        import_name,
        connection_factory,
        *,
        loader=None,
        planner_class=SimplePlanner,
    ):
        self.import_name = import_name
        self.connection_factory = connection_factory
        self.loader = loader or SQLLoader(import_name)
        self.planner_class = planner_class

        self._migrations = None
        self._versions = None
        self._planner = None

    def load(self):
        if self._migrations is not None:
            return False

        self._migrations = {m.version: m for m in self.loader.load_all()}
        self._versions = list(self._migrations.keys())
        self._planner = self.planner_class(self._versions)

        if self.loader.errors:
            raise LoadFailed(
                "load failed",
                errors=self.loader.errors,
                warnings=self.loader.warnings,
            )

        return True

    def run_cli(self, args=None, **kwargs):
        cli = _CLI(self, **kwargs)
        sys.exit(cli(args))

    def up(
        self,
        target=None,
        isolation_level=None,
        num_retries=0,
        progress_callback=None,
    ):
        self._check_loaded()

        if target is None:
            target = self._versions[-1]

        return self._migrate(
            target,
            isolation_level=isolation_level,
            num_retries=num_retries,
            progress_callback=progress_callback,
        )

    def down(
        self,
        target=None,
        isolation_level=None,
        num_retries=0,
        progress_callback=None,
    ):
        self._check_loaded()

        if target is None:
            target = -1

        return self._migrate(
            target,
            isolation_level=isolation_level,
            num_retries=num_retries,
            progress_callback=progress_callback,
        )

    def _migrate(
        self, target, *, isolation_level, num_retries, progress_callback
    ):
        with self.connection_factory() as conn:
            conn.set_session(autocommit=True)
            self._log.debug("creating history table if not exists")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS __kirameki_history__ (
                        version integer PRIMARY KEY,
                        sha256 character(64) NOT NULL,
                        applied_on timestamp DEFAULT (now() at time zone 'utc') NOT NULL
                    )
                    """
                )
            conn.set_session(autocommit=False, isolation_level=isolation_level)
            retry = num_retries + 1
            while retry:
                self._log.info("attempt #%s", num_retries - retry + 1)
                with conn.cursor() as cur:
                    self._log.info(
                        "acquiring access exclusive lock on history table"
                    )
                    cur.execute(
                        "LOCK TABLE __kirameki_history__ IN ACCESS EXCLUSIVE MODE"
                    )
                    cur.execute("SELECT version FROM __kirameki_history__")
                    state = [v for (v,) in cur]
                    plan, direction, cur, tgt = self._planner.plan(
                        state, target
                    )
                    self._log.info("migrating from %s to %s", cur, tgt)
                    if direction is PlanDirection.FORWARD:
                        step = self._apply_forwards
                    elif direction is PlanDirection.BACKWARD:
                        step = self._apply_backwards
                        for ver in plan:
                            if not self._migrations[ver].downable:
                                raise PlanningError(
                                    "cannot proceed: version {} is not downable".format(
                                        ver
                                    )
                                )
                    elif direction is PlanDirection.UNCHANGED:
                        return
                    else:  # pragma: no cover
                        raise RuntimeError("???")
                    try:
                        for ver in plan:
                            m = self._migrations[ver]
                            step(conn, m)
                            self._progress(progress_callback, m.version, True)
                    except Exception:
                        self._progress(progress_callback, m.version, False)
                        conn.rollback()
                        raise
                    try:
                        conn.commit()
                    except errors.SerializationFailure:
                        self._log.warning("serialization failure, retrying...")
                        retry -= 1
                        conn.rollback()
                        continue
                    break

    def _progress(self, progress_callback, *args):
        if progress_callback is not None:
            try:
                progress_callback(*args)
            except Exception:
                self._log.error(
                    "swallowing exception generated by progress_callback",
                    exc_info=True,
                )

    def _apply_forwards(self, conn, m):
        self._log.info("up %s", m.version)
        m.up(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO __kirameki_history__ (
                    version,
                    sha256
                )
                VALUES (%s, %s)
                """,
                (m.version, m.sha256),
            )

    def _apply_backwards(self, conn, m):
        self._log.info("down %s", m.version)
        m.down(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM  __kirameki_history__
                WHERE version = %s
                """,
                (m.version,),
            )

    def _check_loaded(self):
        if self._migrations is None:
            raise RuntimeError(
                "call #load() before attempting migration operations"
            )
