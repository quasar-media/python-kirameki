from contextlib import closing

import pytest

from kirameki import testing


def test_temporary_database(conn, cur):
    with testing.TemporaryDatabase(conn) as tmpdb:
        with closing(tmpdb.connect()) as conn_:
            assert conn_.info.dbname == tmpdb.name
        name = tmpdb.name
    cur.execute(
        "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (name,)
    )
    assert not cur.fetchone()

    with testing.TemporaryDatabase(conn) as tmpdb:
        with pytest.raises(RuntimeError, match="dbname"):
            tmpdb.connect(dbname="foobar")
        with closing(tmpdb.connect(application_name="foobar")) as conn_:
            assert conn_.info.dsn_parameters["application_name"] == "foobar"
        with tmpdb:
            with closing(tmpdb.connect()) as conn_:
                assert conn_.info.dbname == tmpdb.name
    tmpdb.dropdb()
    with pytest.raises(RuntimeError, match="closed"):
        tmpdb.createdb()

    tmpdb = testing.TemporaryDatabase(conn)
    with pytest.raises(RuntimeError, match="no database"):
        tmpdb.connect()
