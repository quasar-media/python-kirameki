import uuid

import psycopg2
from psycopg2 import errors, extensions

from kirameki.extras import set_session

DEFAULT_CREATEDB_STMT = "CREATE DATABASE {name}"


class TemporaryDatabase:
    def __init__(self, _conn, createdb_stmt=None, createdb_vars=()):
        self.createdb_stmt = createdb_stmt or DEFAULT_CREATEDB_STMT
        self.createdb_vars = createdb_vars

        self._name = None
        self._conn = _conn

    @property
    def closed(self):
        return self._conn is None

    @property
    def name(self):
        return self._name

    @property
    def _qname(self):
        # XXX(auri): probably unnecessary but better safe than sorry
        return extensions.quote_ident(self._name, self._conn)

    def createdb(self):
        if self._name is not None:
            return
        self._check_closed()
        while True:
            self._name = uuid.uuid4().hex
            try:
                with set_session(
                    self._conn, autocommit=True
                ), self._conn.cursor() as cur:
                    cur.execute(
                        self.createdb_stmt.format(name=self._qname),
                        self.createdb_vars,
                    )
                break
            except errors.DuplicateDatabase:
                pass

    def dsn_parameters(self, **kwargs):
        if self._name is None:
            raise RuntimeError("no database")
        if "dbname" in kwargs:
            raise RuntimeError("dbname not allowed")
        dsn_parameters = self._conn.info.dsn_parameters
        dsn_parameters.update(
            password=self._conn.info.password,
            dbname=self._name,
        )
        dsn_parameters.update(kwargs)
        return dsn_parameters

    def dsn(self, **kwargs):
        return extensions.make_dsn(**self.dsn_parameters(**kwargs))

    def connect(self, **kwargs):
        kwds = self.dsn_parameters(**kwargs)
        kwds.setdefault("connection_factory", type(self._conn))
        kwds.setdefault("cursor_factory", self._conn.cursor_factory)
        return psycopg2.connect(**kwds)

    def dropdb(self):
        if self._name is None:
            return
        self._check_closed()
        try:
            with set_session(
                self._conn, autocommit=True
            ), self._conn.cursor() as cur:
                cur.execute("DROP DATABASE {}".format(self._qname))
        except errors.ObjectInUse:
            raise RuntimeError(
                "database in use: disconnect all clients before "
                "attempting cleanup"
            )
        self._name = None

    def close(self):
        self.dropdb()
        self._conn = None

    def _check_closed(self):
        if self.closed:
            raise RuntimeError("closed")

    def __enter__(self):
        self.createdb()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not self.closed:
            self.close()
