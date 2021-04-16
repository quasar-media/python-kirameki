import os.path
import shutil
import sys
import tempfile
from contextlib import ExitStack, closing

from kirameki import testing

import psycopg2
import pytest

TEST_ROOT = os.path.dirname(__file__)


@pytest.fixture
def _conn_args(request):
    args, kwargs = ("",), {}
    marker = request.node.get_closest_marker("conn_args")
    if marker is not None:
        args += marker.args
        kwargs.update(marker.kwargs)
    return args, kwargs


@pytest.fixture
def conn(_conn_args):
    args, kwargs = _conn_args
    with closing(psycopg2.connect(*args, **kwargs)) as conn:
        try:
            yield conn
        finally:
            conn.rollback()


@pytest.fixture
def cur(conn):
    with conn.cursor() as cur:
        yield cur


@pytest.fixture
def tmpdb(_conn_args):
    args, kwargs = _conn_args
    with testing.TemporaryDatabase(*args, **kwargs) as tmpdb:
        yield tmpdb


@pytest.fixture
def tmpdb_conn(tmpdb):
    with closing(tmpdb.connect()) as conn:
        yield conn


@pytest.fixture
def casedirs(request):
    # XXX(auri): dirs_exist_ok is not present
    # in python 3.7 and below
    if sys.hexversion < 0x030800F0:
        pytest.skip("cannot use casedirs on Python <3.8")
    marker = request.node.get_closest_marker("casedirs")
    if marker is None:
        raise RuntimeError("cannot provide casedirs without casedirs mark")
    paths = [os.path.join(TEST_ROOT, "casefiles", p) for p in marker.args]
    with ExitStack() as stack:
        tempdirs = [
            stack.enter_context(tempfile.TemporaryDirectory()) for _ in paths
        ]
        for p, td in zip(paths, tempdirs):
            shutil.copytree(p, td, dirs_exist_ok=True)
        yield tempdirs
