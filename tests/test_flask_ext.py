import pytest

try:
    import flask
except ImportError:
    flask = None
else:
    import kirameki.flask_ext

import kirameki.exc


class _DummyConnection:
    pass


class _DummyCursor:
    pass


@pytest.fixture
def app():
    return flask.Flask(__name__)


@pytest.mark.skipif(flask is None, reason="requires [flask] extra")
class TestPooledDatababse:
    @pytest.fixture
    def unyield_mock_db(self, app, mocker):
        pool_class = mocker.MagicMock()
        db = kirameki.flask_ext.PooledDatabase(
            app,
            pool_class=pool_class,
            pool_kwargs={
                # we need this here in order to not
                # trigger the warning for when it is
                # not set
                "connection_factory": None,
                "foo": 42,
            },
        )
        return db, pool_class

    @pytest.fixture
    def mock_db(self, unyield_mock_db):
        with unyield_mock_db[1]:
            yield unyield_mock_db

    def test_pool_creation(self, mock_db):
        _, pool_class = mock_db
        pool_class.assert_called_once_with(connection_factory=None, foo=42)

    def test_stable_get_connection(self, app, mock_db, mocker):
        db, pool_class = mock_db

        _conn = object()
        pool_class().get_connection.return_value = _conn

        with app.app_context():
            conn1 = db.get_connection(bar="bar")
            conn2 = db.get_connection(bar="baz")
            assert conn1 is conn2

        # make sure connections aren't issued for no reason
        # and ensure teardown_appcontext no-op branch operates
        # properly
        with app.app_context():
            pass

        pool_class().get_connection.assert_has_calls([mocker.call(bar="bar")])
        pool_class().return_connection.assert_called_once_with(_conn)

    def test_pool_not_initialized(self):
        db = kirameki.flask_ext.PooledDatabase()

        with pytest.raises(RuntimeError, match="not initialized"):
            db.get_connection()

        with pytest.raises(RuntimeError, match="not initialized"):
            db.__enter__()

    def test_pool_closed_on_exit(self, unyield_mock_db):
        db, pool_class = unyield_mock_db
        pool_class().closed.return_value = False

        with db:
            pass

        pool_class().close.assert_called_once()

    def test_reinitializing(self, app, mock_db):
        db, pool_class = mock_db

        with pytest.warns(kirameki.exc.KiramekiWarning, match="reinit"):
            db.init_app(app)

        app.config["TESTING"] = True
        pool_class().closed.return_value = False
        db.init_app(app)
        pool_class().close.assert_called_once()

    def test_make_connection_factory(self, app, mocker):
        pool_class = mocker.MagicMock()

        m = __name__
        dsn = "user=foo"
        app.config["PQ_DSN"] = dsn
        app.config["PSYCOPG2_CONNECTION_FACTORY"] = m + "._DummyConnection"
        app.config["PSYCOPG2_CURSOR_FACTORY"] = m + "._DummyCursor"
        _ = kirameki.flask_ext.PooledDatabase(app, pool_class=pool_class)

        pool_class.assert_called_once()

        factory = pool_class.call_args.kwargs["connection_factory"]
        connect = mocker.patch("psycopg2.connect")
        factory()
        connect.assert_called_once_with(
            dsn,
            connection_factory=_DummyConnection,
            cursor_factory=_DummyCursor,
        )
