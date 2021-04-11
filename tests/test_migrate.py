import sys

import pytest

from kirameki.migrate import PyLoader


class TestPyLoader:
    @pytest.mark.casedirs("migrate/test_pyloader/validate_0")
    def test_valid_0(self, casedirs):
        (d,) = casedirs
        sys.path.insert(0, d)
        try:
            loader = PyLoader("test_module")
            migrations = loader.load_all()
            assert not loader.errors
            assert not loader.warnings
            migrations = [
                (m.version, m.description, m.downable) for m in migrations
            ]
            assert migrations == [
                (1618113091, "initial", True),
                (1618113298, "one", True),
                (1618113302, "two", False),
                (1618113304, "three", False),
            ]
        finally:
            sys.path.remove(d)
