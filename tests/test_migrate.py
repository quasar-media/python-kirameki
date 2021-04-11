import os
import os.path
import pkgutil
import sys

import pytest

from kirameki.migrate import PyLoader


class TestPyLoader:
    @pytest.mark.casedirs("migrate/test_pyloader/validate_0")
    def test_valid_0(self, casedirs):
        (d,) = casedirs
        loader = PyLoader(
            "test_module", root_path=os.path.join(d, "test_module")
        )
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

    def test_get_root_path(self):
        loader = PyLoader(__name__)
        assert loader.root_path == os.path.dirname(__file__)

    @pytest.mark.casedirs("migrate/test_pyloader/test_get_root_path")
    def test_get_root_path_unloaded(self, casedirs, monkeypatch):
        (d,) = casedirs
        monkeypatch.syspath_prepend(d)
        loader = PyLoader("_unloaded")
        assert loader.root_path == d

    def test_get_root_path_unloaded_main(self, monkeypatch):
        monkeypatch.delitem(sys.modules, "__main__")
        loader = PyLoader("__main__")
        assert loader.root_path == os.getcwd()

    def test_get_root_path_no_get_filename(self, monkeypatch):
        loader = pkgutil.find_loader(__name__)
        # NOTE(auri): this is probably unnecesary cause
        # pytest importer doesn't have get_filename but
        # we get rid of it just in case that changes
        monkeypatch.delattr(loader, "get_filename", raising=False)
        monkeypatch.delitem(sys.modules, __name__)

        with pytest.raises(RuntimeError, match="root_path"):
            PyLoader(__name__)
