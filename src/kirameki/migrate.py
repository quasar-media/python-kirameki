import collections
import functools
import importlib
import os
import os.path
import pkgutil
import sys


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
