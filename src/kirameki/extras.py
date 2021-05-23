import warnings
from collections import namedtuple
from contextlib import contextmanager

import psycopg2
from psycopg2 import extensions

from kirameki import exc

#: A :func:`namedtuple` representing the result of a
#: :meth:`~kirameki.extras.SimpleConnectionMixin.execute` call.
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


class Savepoint(_TransactionMixin):
    """A savepoint context manager.
    """

    __slots__ = ("_conn", "_ident")

    def __init__(self, conn, name):
        self._conn = conn
        self._ident = extensions.quote_ident(name, conn)

    def savepoint(self, name):
        """Create a new, nested savepoint of the same type.

        :param name: savepoint name
        :type name: str
        """

        return type(self)(self._conn, name)

    def begin(self):
        """Create a savepoint. You should only call this method after
        calling :meth:`commit` with `begin=False`.
        """

        self._conn.execute("SAVEPOINT {}".format(self._ident))

    def commit(self, begin=True):
        """Release the savepoint.

        :param begin: create a new savepoint if this savepoint is released
            successfully, defaults to True
        :type begin: bool, optional
        """

        self._conn.execute("RELEASE SAVEPOINT {}".format(self._ident))
        if begin:
            self.begin()

    def rollback(self):
        """Rollback the savepoint.
        """

        self._conn.execute("ROLLBACK TO SAVEPOINT {}".format(self._ident))


class Transaction(_TransactionMixin):
    """A transaction context manager.
    """

    #: The savepoint class to use.
    savepoint_class = Savepoint

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
        """Start a savepoint context.

        :param name: savepoint name
        :type name: str

        :returns: a savepoint control object that is also a context manager
        """

        return self.savepoint_class(self._conn, name)

    def begin(self):
        """Begin a transaction block. You should only call this method after
        calling :meth:`commit` with `begin=False`.
        """

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
        """Commit the transaction.

        :param begin: begin a new transaction if this transaction commits
            successfully, defaults to True
        :type begin: bool, optional
        """

        try:
            self._conn.commit()
        finally:
            self._reset()
        if begin:
            self.begin()

    def rollback(self):
        """Rollback the migration.
        """

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
    """A :class:`connection` mixin providing helper methods.
    """

    #: The transaction class.
    transaction_class = Transaction

    def transaction(
        self,
        isolation_level=None,
        readonly=None,
        deferrable=None,
    ):
        """Start a transaction context.

        See :meth:`connection.set_session` for more information.

        :param isolation_level: set session isolation level in this context,
            default behavior is to not change current isolation level i.e.
            None
        :type isolation_level: Union[str, int], optional
        :param readonly: make session read-only in this context, default
            behavior is to not change current read-only state
        :type readonly: bool, optional
        :param deferrable: make session deferrable in this context, default
            behavior is to not change current deferrable state
        :type deferrable: bool, optional

        :raises psycopg2.OperationalError: when invalid values are provided
            or the database state is invalid (e.g. in-transaction)

        :return: a transactional control object that is also a context manager
        :rtype: Transaction
        """

        if isolation_level is extensions.ISOLATION_LEVEL_AUTOCOMMIT:
            raise psycopg2.OperationalError(
                "cannot use autocommit in transaction context"
            )

        if self.info.transaction_status != extensions.TRANSACTION_STATUS_IDLE:
            raise psycopg2.OperationalError(
                "cannot start a transaction while "
                "one is already in progress (connection not idle)"
            )

        return self.transaction_class(
            self,
            isolation_level,
            readonly,
            deferrable,
        )

    def execute(self, query, vars=None):
        """Execute a single query without constructing a cursor.

        See :meth:`cursor.execute`.

        :returns: a namedtuple providing two fields: `lastrowid` and
            `rowcount`.
        :rtype: Result
        """

        with self.cursor() as cur:
            cur.execute(query, vars)
        return Result(lastrowid=cur.lastrowid, rowcount=cur.rowcount)

    def callproc(self, procname, parameters=None):
        """Call a single stored procedure without constructing a cursor.

        See :meth:`cursor.callproc`.

        :returns: an iterator over rows returned by the call.
        """

        with self.cursor() as cur:
            cur.callproc(procname, parameters)
            yield from cur

    def callproc_one(self, procname, parameters=None):
        """Call a single stored procedure without constructing a cursor
        and return a single row.

        See :meth:`cursor.callproc`. Warns when more than one row is returned.

        :returns: a single row
        """

        with self.cursor() as cur:
            cur.callproc(procname, parameters)
            return self._ensure_one(cur)

    def query(self, query, vars=None):
        """Execute a single query without constructing a cursor.

        See :meth:`cursor.execute`.

        :returns: an iterator over rows returned by the call.
        """

        with self.cursor() as cur:
            cur.execute(query, vars)
            yield from cur

    def query_one(self, query, vars=None):
        """Execute a single query without constructing a cursor.

        See :meth:`cursor.execute`. Warns when more than one row is returned.

        :returns: a single row
        """

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
    """A class inheriting from :class:`connection` and
    :class:`SimpleConnectionMixin`.
    """

    pass


@contextmanager
def set_session(conn, **kwargs):
    """Set session parameters within a context.

    This context manager sets given session parameters on enter and restores
    them to their previous values on exit.

    See :meth:`connection.set_session`.
    """

    stateargs = {k: getattr(conn, k) for k in kwargs}
    conn.set_session(**kwargs)
    try:
        yield
    finally:
        conn.set_session(**stateargs)
