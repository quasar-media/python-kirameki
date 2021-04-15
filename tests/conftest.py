from contextlib import closing

import psycopg2
import pytest


@pytest.fixture
def conn(request):
    args, kwargs = ("",), {}
    marker = request.node.get_closest_marker("conn_args")
    if marker is not None:
        args += marker.args
        kwargs.update(marker.kwargs)
    with closing(psycopg2.connect(*args, **kwargs)) as conn:
        try:
            yield conn
        finally:
            conn.rollback()


@pytest.fixture
def cur(conn):
    with conn.cursor() as cur:
        yield cur
