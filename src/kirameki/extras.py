import uuid
import warnings
from collections import namedtuple
from contextlib import contextmanager

import psycopg2
from psycopg2 import extensions

from kirameki import exc

Result = namedtuple("Result", "lastrowid rowcount")


class _TransactionMixin:
    __slots__ = ()

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # NOTE(auri): we, unlike most orms, rollback unconditionally:
        # the user needs to commit explicitly
        self.rollback()


class _Savepoint(_TransactionMixin):
    __slots__ = (
        "isolation_level",
        "readonly",
        "deferrable",
        "_conn",
        "_ident",
    )

    def __init__(self, conn, name, isolation_level, readonly, deferrable):
        self.isolation_level = isolation_level
        self.readonly = readonly
        self.deferrable = deferrable
        self._conn = conn
        self._ident = extensions.quote_ident(name, conn)

    def savepoint(self, name):
        return type(self)(
            self._conn,
            name,
            self.isolation_level,
            self.readonly,
            self.deferrable,
        )

    def begin(self):
        self._conn.execute("SAVEPOINT {}".format(self._ident))

    def commit(self, begin=True):
        self._conn.execute("RELEASE SAVEPOINT {}".format(self._ident))
        if begin:
            self.begin()

    def rollback(self):
        self._conn.execute("ROLLBACK TO SAVEPOINT {}".format(self._ident))


class _Transaction(_TransactionMixin):
    savepoint_class = _Savepoint

    __slots__ = (
        "isolation_level",
        "readonly",
        "deferrable",
        "_conn",
        "_save_autocommit",
        "_save_isolation_level",
        "_save_readonly",
        "_save_deferrable",
    )

    def __init__(self, conn, isolation_level, readonly, deferrable):
        self.isolation_level = isolation_level
        self.readonly = readonly
        self.deferrable = deferrable
        self._conn = conn
        self._save_autocommit = None
        self._save_isolation_level = None
        self._save_readonly = None
        self._save_deferrable = None

    def savepoint(self, name):
        return self.savepoint_class(
            self._conn,
            name,
            self.isolation_level,
            self.readonly,
            self.deferrable,
        )

    def begin(self):
        self._save_autocommit = self._conn.autocommit
        self._save_isolation_level = self._conn.isolation_level
        self._save_readonly = self._conn.readonly
        self._save_deferrable = self._conn.deferrable
        self._conn.set_session(
            autocommit=False,
            isolation_level=self.isolation_level,
            readonly=self.readonly,
            deferrable=self.deferrable,
        )

    def commit(self, begin=True):
        try:
            self._conn.commit()
        finally:
            self._reset()
        if begin:
            self.begin()

    def rollback(self):
        try:
            self._conn.rollback()
        finally:
            self._reset()

    def _reset(self):
        try:
            self._conn.set_session(
                autocommit=self._save_autocommit,
                isolation_level=self._save_isolation_level,
                readonly=self._save_readonly,
                deferrable=self._save_deferrable,
            )
        finally:
            self._save_autocommit = None
            self._save_isolation_level = None
            self._save_readonly = None
            self._save_deferrable = None


class SimpleConnectionMixin:
    transaction_class = _Transaction

    _transaction = None

    def atomic(
        self, name=None, isolation_level=None, readonly=None, deferrable=None
    ):
        if isolation_level is extensions.ISOLATION_LEVEL_AUTOCOMMIT:
            raise psycopg2.OperationalError(
                "cannot use autocommit in transaction context"
            )
        if self._transaction:
            if any(
                filter(
                    lambda v: v is not None,
                    (isolation_level, readonly, deferrable),
                )
            ):
                warnings.warn(
                    "cannot change characteristics mid-transaction "
                    "(i.e. when creating a savepoint)",
                    exc.KiramekiWarning,
                    stacklevel=2,
                )
            name = name or uuid.uuid4().hex
            return self._transaction.savepoint(name)
        return self._atomic(isolation_level, readonly, deferrable)

    @contextmanager
    def _atomic(self, isolation_level, readonly, deferrable):
        if self.info.transaction_status != extensions.TRANSACTION_STATUS_IDLE:
            raise psycopg2.OperationalError(
                "cannot start a transaction while "
                "one is already in progress (connection not idle)"
            )
        self._transaction = self.transaction_class(
            self, isolation_level, readonly, deferrable
        )
        try:
            with self._transaction:
                yield self._transaction
        finally:
            self._transaction = None

    def execute(self, query, vars=None):
        with self.cursor() as cur:
            cur.execute(query, vars)
        return Result(lastrowid=cur.lastrowid, rowcount=cur.rowcount)

    def callproc(self, procname, parameters=None):
        with self.cursor() as cur:
            cur.callproc(procname, parameters)
            yield from cur

    def callproc_one(self, procname, parameters=None):
        with self.cursor() as cur:
            cur.callproc(procname, parameters)
            return self._ensure_one(cur)

    def query(self, query, vars=None):
        with self.cursor() as cur:
            cur.execute(query, vars)
            yield from cur

    def query_one(self, query, vars=None):
        with self.cursor() as cur:
            cur.execute(query, vars)
            return self._ensure_one(cur)

    def _ensure_one(self, cur):
        rows = cur.fetchall()
        if not rows:
            return None
        try:
            (row,) = rows
            return row
        except ValueError:
            warnings.warn(
                "query generated more than one row",
                exc.KiramekiWarning,
                stacklevel=3,
            )
            return rows[0]


class SimpleConnection(extensions.connection, SimpleConnectionMixin):
    pass


@contextmanager
def set_session(conn, **kwargs):
    stateargs = {k: getattr(conn, k) for k in kwargs}
    conn.set_session(**kwargs)
    try:
        yield
    finally:
        conn.set_session(**stateargs)
