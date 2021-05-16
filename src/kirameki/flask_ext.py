import importlib
import logging
import warnings

import flask
import psycopg2

from kirameki import exc
from kirameki import pool as _pool


class PooledDatabase:
    connection_key = "_kirameki_connection"

    _log = logging.getLogger(__qualname__)

    __slots__ = ("app", "pool_class", "pool_kwargs", "_pool")

    def __init__(
        self,
        app=None,
        pool_class=_pool.PriorityPool,
        pool_kwargs=None,
    ):
        self.app = None
        self.pool_class = pool_class
        self.pool_kwargs = pool_kwargs or {}

        self._pool = None

        if app is not None:
            self.init_app(app)

    def get_connection(self, **kwargs):
        if self._pool is None:
            raise RuntimeError("database not initialized")

        try:
            return getattr(flask.g, self.connection_key)
        except AttributeError:
            conn = self._pool.get_connection(**kwargs)
            setattr(flask.g, self.connection_key, conn)
            return conn

    def init_app(self, app):
        if self._pool is not None:
            if not (self.app.config["TESTING"] and app.config["TESTING"]):
                warnings.warn(
                    "reinitializing database when not in testing mode",
                    category=exc.KiramekiWarning,
                )
            self._log.debug("reinitializing")
            if not self._pool.closed():
                self._log.debug("closing old pool")
                self._pool.close()

        self.app = app
        self._pool = self.pool_class(**self._make_pool_kwargs(app))

        @self.app.teardown_appcontext
        def return_connection(e):
            conn = getattr(flask.g, self.connection_key, None)
            if conn is None:
                return
            self._pool.return_connection(conn)

    def _make_pool_kwargs(self, app):
        kwds = self.pool_kwargs.copy()
        if "connection_factory" not in kwds:
            # try and see if the app can provide us with
            # connection details
            if "PQ_DSN" in app.config:
                kwds["connection_factory"] = self._make_connection_factory(app)
            else:  # pragma: no cover
                # guess not; warn so that it's clear where the
                # pool instantiation failure is coming from
                warnings.warn(
                    "no connection_factory provided in pool_kwargs and "
                    "no PQ_DSN in app config, pool instatiation will fail",
                    category=exc.KiramekiWarning,
                )
        return kwds

    def _make_connection_factory(self, app):
        def _import(key):
            class_path = app.config.get(key)
            if class_path is None:
                return None
            mod_name, _, class_name = class_path.rpartition(".")
            if not (mod_name or class_name):
                # TODO: better error
                raise ValueError("invalid class path: {}".format(class_path))
            mod = importlib.import_module(mod_name)
            return getattr(mod, class_name)

        dsn = app.config["PQ_DSN"]
        _connection_factory = _import("PSYCOPG2_CONNECTION_FACTORY")
        _cursor_factory = _import("PSYCOPG2_CURSOR_FACTORY")

        def connection_factory():
            return psycopg2.connect(
                dsn,
                connection_factory=_connection_factory,
                cursor_factory=_cursor_factory,
            )

        return connection_factory

    def __enter__(self):
        if self._pool is None:
            raise RuntimeError("database not initialized")

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not self._pool.closed():
            self._pool.close()
