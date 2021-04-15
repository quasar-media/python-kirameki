from contextlib import closing

import pytest

from kirameki import testing


def test_temporary_database(cur):
    with testing.TemporaryDatabase("") as tmpdb:
        with closing(tmpdb.connect()) as conn:
            assert conn.info.dbname == tmpdb.name
        conn.close()
        name = tmpdb.name
    cur.execute(
        "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (name,)
    )
    assert not cur.fetchone()

    with testing.TemporaryDatabase("") as tmpdb:
        with pytest.raises(RuntimeError, match="dbname"):
            tmpdb.connect(dbname="foobar")
        with tmpdb:
            with closing(tmpdb.connect()) as conn:
                assert conn.info.dbname == tmpdb.name
    tmpdb.dropdb()
    with pytest.raises(RuntimeError, match="closed"):
        tmpdb.createdb()

    tmpdb = testing.TemporaryDatabase("")
    with pytest.raises(RuntimeError, match="no database"):
        tmpdb.connect()
