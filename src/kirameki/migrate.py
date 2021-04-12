import collections
import enum
import functools
import importlib
import logging
import os
import os.path
import pkgutil
import sys

from psycopg2 import errors


class Migration:
    def __init__(self, version, description=None):
        self.version = version
        self.description = description

    @classmethod
    def from_callables(cls, up, down=None, _module=None, **kwargs):
        def make_wrapper(func):
            @functools.wraps(func)
            def wrapper(self, conn):
                return func(conn)

            return wrapper

        if _module is None:
            _module = __name__

        ns = {"up": make_wrapper(up), "__module__": _module}
        if down is not None:
            ns["down"] = make_wrapper(down)
        cls_ = type("_synthetic_", (cls,), ns)
        return cls_(**kwargs)

    @property
    def downable(self):
        return self.down is not None

    def up(self, conn):
        raise NotImplementedError()

    down = None

    def __repr__(self):
        s = "<migration version={!r} description={!r} uppable"
        if self.downable:
            s += " downable"
        s += ">"
        return s.format(self.version, self.description)


class _LoadFault:
    def __init__(self, msg, name):
        super().__init__(msg, name)
        self.msg = msg
        self.name = name

    def __str__(self):
        return "{}: {}".format(self.name, self.msg)


class LoadError(_LoadFault, Exception):
    pass


class LoadWarning(_LoadFault, Warning):
    pass


class Loader:
    migration_class = Migration

    def __init__(self, import_name):
        self.import_name = import_name
        self.errors = collections.OrderedDict()
        self.warnings = collections.OrderedDict()

    def load_all(self):
        raise NotImplementedError()

    def _report_error(self, name, msg, cause=None):
        e = LoadError(msg, name)
        e.__cause__ = cause
        try:
            errors = self.errors[name]
        except KeyError:
            errors = self.errors[name] = []
        errors.append(e)

    def _report_warning(self, name, msg):
        w = LoadWarning(msg, name)
        try:
            warnings = self.warnings[name]
        except KeyError:
            warnings = self.warnings[name] = []
        warnings.append(w)


class PyLoader(Loader):
    def __init__(self, *args, root_path=None, migration_pkg=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.root_path = root_path or self._get_root_path()
        self.migration_pkg = migration_pkg or "migrations"

    def load_all(self):
        for mod in self._collect_modules():
            version = self._get_module_attr(
                mod,
                "version",
                True,
                lambda v: isinstance(v, int),
                "version must be an int",
            )
            description = self._get_module_attr(
                mod,
                "description",
                False,
                lambda v: isinstance(v, str),
                "description must be a string",
            )
            up = self._get_module_attr(
                mod, "up", True, callable, "up must be callable"
            )
            down = self._get_module_attr(
                mod, "down", False, callable, "down must be callable"
            )
            if version is None or up is None:
                continue
            yield self.migration_class.from_callables(
                up,
                down,
                _module=mod.__name__,
                version=version,
                description=description,
            )

    def _get_module_attr(self, mod, name, required, check, check_msg):
        try:
            value = getattr(mod, name)
        except AttributeError as e:
            if required:
                self._report_error(
                    mod.__name__, "must define {}".format(name), cause=e
                )
            return None
        if not check(value):
            self._report_error(mod.__name__, check_msg)
            return None
        return value

    def _collect_modules(self):
        mdir = os.path.join(self.root_path, self.migration_pkg)
        if not os.path.isdir(mdir):
            raise RuntimeError("not a directory: {}".format(mdir))

        import_path = os.path.dirname(self.root_path)
        sys.path.insert(0, import_path)
        try:
            modules = []
            for modinfo in pkgutil.iter_modules(
                [mdir],
                prefix="{}.{}.".format(self.import_name, self.migration_pkg),
            ):
                name = modinfo.name
                if modinfo.ispkg:
                    self._report_warning(name, "is a package, ignoring")
                    continue
                try:
                    # XXX(auri): if we could avoid this call and instead be
                    # able to find_spec/module_from_spec/exec_module
                    # without going around the system cache, it'd be more
                    # ideal than the sys.path workaround
                    mod = importlib.import_module(name)
                except Exception as e:
                    self._report_error(name, "failed to import", cause=e)
                    continue
                modules.append(mod)
        finally:
            sys.path.remove(import_path)

        # XXX(auri): this method should always be eagerly evaluated
        # to ensure we never leave the import_path within sys.path
        return modules

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
        target = max(target, 0)

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


class LoadFailed(RuntimeError):
    pass


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
        self.loader = loader or PyLoader(import_name)
        self.planner_class = planner_class

        self._migrations = {m.version: m for m in self.loader.load_all()}
        self._versions = list(self._migrations.keys())
        self._planner = planner_class(self._versions)

        # TODO(auri): loader.warnings
        if self.loader.errors:
            # TODO(auri): something more pretty
            raise LoadFailed(self.loader.errors)

    def up(self, target=None, isolation_level=None, num_retries=0):
        if target is None:
            target = self._versions[-1]

        return self._migrate(
            target,
            isolation_level=isolation_level,
            num_retries=num_retries,
        )

    def down(self, target=None, isolation_level=None, num_retries=0):
        if target is None:
            target = -1

        return self._migrate(
            target,
            isolation_level=isolation_level,
            num_retries=num_retries,
        )

    def _migrate(self, target, *, isolation_level, num_retries):
        with self.connection_factory() as conn:
            conn.set_session(autocommit=True)
            self._log.debug("creating history table if not exists")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS __kirameki_history__ (
                        version integer PRIMARY KEY,
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
                    except Exception:
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

    def _apply_forwards(self, conn, m):
        self._log.info("up %s", m.version)
        m.up(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO __kirameki_history__ (
                    version
                )
                VALUES (%s)
                """,
                (m.version,),
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
