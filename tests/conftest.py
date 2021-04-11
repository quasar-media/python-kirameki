import os.path
import shutil
import tempfile
from contextlib import ExitStack, closing

import psycopg2
import pytest

TEST_ROOT = os.path.dirname(__file__)


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


@pytest.fixture
def casedirs(request):
    marker = request.node.get_closest_marker("casedirs")
    if marker is None:
        raise RuntimeError("cannot provide casedirs without casedirs mark")
    paths = [os.path.join(TEST_ROOT, "casefiles", p) for p in marker.args]
    with ExitStack() as stack:
        tempdirs = [
            stack.enter_context(tempfile.TemporaryDirectory()) for _ in paths
        ]
        for p, td in zip(paths, tempdirs):
            # TODO(auri): dirs_exist_ok fails test on python <3.8
            shutil.copytree(p, td, dirs_exist_ok=True)
        yield tempdirs
