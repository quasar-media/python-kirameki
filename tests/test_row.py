import pytest

from kirameki.rows import RowCursor


@pytest.mark.conn_args(cursor_factory=RowCursor)
def test_row_cursor(cur):
    onetwo = (1, 2)
    cur.execute("SELECT %s AS one, %s AS two", vars=onetwo)
    row = cur.fetchone()
    _sentinel = object()
    assert row.as_dict() == {"one": 1, "two": 2}
    assert row.get("notthere", _sentinel) is _sentinel
    # tuple __eq__
    assert row == onetwo
    # __getattr__
    assert (row.one, row.two) == onetwo
    with pytest.raises(AttributeError):
        row.three
    # __getitem__
    assert (row["one"], row["two"]) == onetwo
    assert (row[0], row[1]) == onetwo
    assert cur.fetchone() is None
    with pytest.raises(KeyError):
        row["notthere"]
    with pytest.raises(IndexError):
        row[2]
    # TODO: executemany

    cur.callproc("generate_series", parameters=(0, 4))
    rows = cur.fetchmany(size=3)
    assert rows[0].as_dict() == {"generate_series": 0}
    assert rows == [(0,), (1,), (2,)]

    cur.arraysize = 2
    rows = cur.fetchmany()
    assert rows == [(3,), (4,)]

    cur.execute("SELECT generate_series(2, 4) AS x")
    rows = cur.fetchall()
    assert rows[0].as_dict() == {"x": 2}
    assert rows == [(2,), (3,), (4,)]

    cur.execute("SELECT generate_series(5, 7) AS y")
    rows = list(cur)
    assert rows[0].as_dict() == {"y": 5}
    assert rows == [(5,), (6,), (7,)]
