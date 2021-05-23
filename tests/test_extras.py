import psycopg2
import pytest
from psycopg2.extensions import (
    ISOLATION_LEVEL_AUTOCOMMIT,
    ISOLATION_LEVEL_READ_COMMITTED,
    ISOLATION_LEVEL_SERIALIZABLE,
    quote_ident,
)

from kirameki import exc
from kirameki.extras import SimpleConnection, set_session


@pytest.mark.conn_args(connection_factory=SimpleConnection)
class TestSimpleConnection:
    def test_transaction(self, conn):
        with pytest.raises(psycopg2.OperationalError, match="autocommit"):
            with conn.transaction(isolation_level=ISOLATION_LEVEL_AUTOCOMMIT):
                pass

        conn.set_session(autocommit=False)
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        with pytest.raises(psycopg2.OperationalError, match="in progress"):
            with conn.transaction():
                pass
        conn.rollback()

        conn.set_session(autocommit=True)
        with conn.transaction():
            assert not conn.autocommit
        assert conn.autocommit

        conn.set_session(
            autocommit=False,
            isolation_level=ISOLATION_LEVEL_SERIALIZABLE,
            readonly=False,
            deferrable=False,
        )
        with conn.transaction(isolation_level=ISOLATION_LEVEL_READ_COMMITTED):
            assert conn.isolation_level == ISOLATION_LEVEL_READ_COMMITTED
        assert conn.isolation_level == ISOLATION_LEVEL_SERIALIZABLE

        with conn.transaction(readonly=True, deferrable=True):
            assert conn.isolation_level == ISOLATION_LEVEL_SERIALIZABLE
            assert conn.readonly and conn.deferrable
        assert not conn.readonly and not conn.deferrable

        with conn.transaction() as txn:
            conn.execute("SET SESSION public.foo TO 42")
            with txn.savepoint("sp") as sp:
                conn.execute("SET SESSION public.bar TO 1337")
                with txn.savepoint("sp2"):
                    conn.execute("SET SESSION public.baz TO 0")
                (baz,) = conn.query_one("SHOW public.baz")
                assert baz == ""
                sp.commit()
            (bar,) = conn.query_one("SHOW public.bar")
            assert bar == "1337"

        with conn.transaction() as txn:
            conn.execute("SET SESSION public.bar TO 1337")
            txn.rollback()

        conn.set_session(autocommit=True)
        for ident in ("public.foo", "public.bar", "public.baz"):
            (val,) = conn.query_one("SHOW {}".format(quote_ident(ident, conn)))
            assert val == ""

        with conn.transaction() as txn:
            conn.execute("SET SESSION public.foo TO 42")
            with txn.savepoint("sp") as sp:
                conn.execute("SET SESSION public.foo TO 1337")
                sp.rollback()
            txn.commit()
        (foo,) = conn.query_one("SHOW public.foo")
        assert foo == "42"

    def test_helpers(self, conn):
        assert (
            list(conn.callproc("generate_series", parameters=(0, 1)))
            == list(conn.query("SELECT generate_series(0, 1)"))
            == [(0,), (1,)]
        )
        assert (
            conn.callproc_one("pg_typeof", (42,))
            == conn.query_one("SELECT pg_typeof(42)")
            == ("integer",)
        )

        assert conn.query_one("SELECT") == ()
        assert conn.query_one("SELECT * FROM generate_series(0, -1)") is None

        with pytest.warns(exc.KiramekiWarning, match="more than one"):
            conn.query_one("SELECT generate_series(0, 10)")


def test_set_session(conn):
    # TODO(auri): test other parameters as well?
    assert not conn.autocommit
    with set_session(conn, autocommit=True):
        assert conn.autocommit
    assert not conn.autocommit
