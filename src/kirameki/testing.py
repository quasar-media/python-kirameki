import uuid

import psycopg2
from psycopg2 import errors, extensions

DEFAULT_CREATEDB_STMT = "CREATE DATABASE {table}"


class TemporaryDatabase:
    def __init__(self, *args, createdb_stmt=None, dbname=None, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.createdb_stmt = createdb_stmt or DEFAULT_CREATEDB_STMT

        self._name = None
        self._conn = psycopg2.connect(*args, dbname=dbname, **kwargs)
        self._conn.set_session(autocommit=True)

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
                with self._conn.cursor() as cur:
                    cur.execute(self.createdb_stmt.format(table=self._qname))
                break
            except errors.DuplicateDatabase:
                pass

    def connect(self, **kwargs):
        if self._name is None:
            raise RuntimeError("no database")
        if "dbname" in kwargs:
            raise RuntimeError("dbname not allowed")
        return psycopg2.connect(
            *self.args, dbname=self._name, **{**self.kwargs, **kwargs}
        )

    def dropdb(self):
        if self._name is None:
            return
        self._check_closed()
        try:
            with self._conn.cursor() as cur:
                cur.execute("DROP DATABASE {}".format(self._qname))
        except errors.ObjectInUse:
            raise RuntimeError(
                "database in use: disconnect all clients before "
                "attempting cleanup"
            )
        self._name = None

    def close(self):
        self.dropdb()
        self._conn.close()
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
