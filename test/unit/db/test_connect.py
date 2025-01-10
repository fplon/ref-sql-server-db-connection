from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from app.db.connect import DatabaseConnection
from app.db.exceptions import DatabaseConnectionError


@pytest.fixture
def db_params() -> dict[str, str | int]:
    """Fixture providing standard database connection parameters."""
    return {
        "db": "test_db",  # str
        "host": "test_host",  # str
        "port": 1433,  # int
        "timeout": 30,  # int
    }


@pytest.fixture
def mock_engine() -> MagicMock:
    """Fixture providing a mock SQLAlchemy engine."""
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = MagicMock()
    return engine


@pytest.fixture
def mock_session() -> MagicMock:
    """Fixture providing a mock SQLAlchemy session."""
    return MagicMock()


@pytest.fixture
def mock_session_maker(mock_session: MagicMock) -> MagicMock:
    """Fixture providing a mock SQLAlchemy sessionmaker."""
    session_maker = MagicMock()
    session_maker.return_value = mock_session
    return session_maker


class TestDatabaseConnection:
    def test_init(self, db_params: dict[str, str | int]) -> None:
        """Test DatabaseConnection initialisation."""
        connection = DatabaseConnection(
            db=str(db_params["db"]),
            host=str(db_params["host"]),
            port=int(db_params["port"]),
            timeout=int(db_params["timeout"]),
        )

        assert connection._db == db_params["db"]
        assert connection._host == db_params["host"]
        assert connection._port == db_params["port"]
        assert connection._timeout == db_params["timeout"]
        assert connection._engine is None
        assert connection._session_maker is None

    @pytest.mark.parametrize(
        "available_drivers,expected_driver",
        [
            (
                ["ODBC Driver 17 for SQL Server", "Other Driver"],
                "ODBC Driver 17 for SQL Server",
            ),
            (
                ["SQL Server Native Client 11.0", "Other Driver"],
                "SQL Server Native Client 11.0",
            ),
            (["ODBC Driver 13 for SQL Server"], "ODBC Driver 13 for SQL Server"),
        ],
    )
    def test_get_available_driver_success(
        self,
        available_drivers: list[str],
        expected_driver: str,
        db_params: dict[str, str | int],
    ) -> None:
        """Test _get_available_driver with various driver configurations."""
        connection = DatabaseConnection(
            db=str(db_params["db"]),
            host=str(db_params["host"]),
            port=int(db_params["port"]),
            timeout=int(db_params["timeout"]),
        )

        with patch("pyodbc.drivers", return_value=available_drivers):
            driver = connection._get_available_driver()
            assert driver == expected_driver

    def test_get_available_driver_no_supported_driver(
        self, db_params: dict[str, str | int]
    ) -> None:
        """Test _get_available_driver when no supported drivers are available."""
        connection = DatabaseConnection(
            db=str(db_params["db"]),
            host=str(db_params["host"]),
            port=int(db_params["port"]),
            timeout=int(db_params["timeout"]),
        )

        with patch("pyodbc.drivers", return_value=["Unsupported Driver"]):
            with pytest.raises(DatabaseConnectionError) as exc_info:
                connection._get_available_driver()

            assert str(exc_info.value) == "No supported ODBC driver found."

    def test_init_connection_success(
        self, db_params: dict[str, str | int], mock_engine: MagicMock
    ) -> None:
        """Test successful database connection initialisation."""
        connection = DatabaseConnection(
            db=str(db_params["db"]),
            host=str(db_params["host"]),
            port=int(db_params["port"]),
            timeout=int(db_params["timeout"]),
        )

        with (
            patch(
                "app.db.connect.create_engine", return_value=mock_engine
            ) as mock_create_engine,
            patch("app.db.connect.sessionmaker") as mock_sessionmaker,
            patch.object(
                connection,
                "_get_available_driver",
                return_value="ODBC Driver 17 for SQL Server",
            ),
            patch("pyodbc.drivers", return_value=["ODBC Driver 17 for SQL Server"]),
        ):
            connection._init_connection()

            expected_connection_string = (
                f"mssql+pyodbc://{db_params['host']}:{db_params['port']}/{db_params['db']}?"
                "driver=ODBC Driver 17 for SQL Server&trusted_connection=yes"
            )

            mock_create_engine.assert_called_once_with(
                expected_connection_string,
                fast_executemany=True,
                connect_args={"timeout": db_params["timeout"]},
            )
            mock_sessionmaker.assert_called_once_with(bind=mock_engine)
            mock_engine.connect.assert_called_once()

    def test_init_connection_retry_success(
        self, db_params: dict[str, str | int], mock_engine: MagicMock
    ) -> None:
        """Test connection initialisation with retry success."""
        connection = DatabaseConnection(
            db=str(db_params["db"]),
            host=str(db_params["host"]),
            port=int(db_params["port"]),
            timeout=int(db_params["timeout"]),
        )

        with (
            patch("app.db.connect.create_engine") as mock_create_engine,
            patch("app.db.connect.sessionmaker") as mock_sessionmaker,
            patch("app.db.connect.sleep") as mock_sleep,
            patch.object(
                connection,
                "_get_available_driver",
                return_value="ODBC Driver 17 for SQL Server",
            ),
            patch("pyodbc.drivers", return_value=["ODBC Driver 17 for SQL Server"]),
        ):
            # First attempt fails, second succeeds
            mock_create_engine.side_effect = [
                OperationalError("mock error", None, None),
                mock_engine,
            ]

            connection._init_connection()

            assert mock_create_engine.call_count == 2
            mock_sleep.assert_called_once_with(1)  # First retry delay
            mock_sessionmaker.assert_called_once_with(bind=mock_engine)

    def test_init_connection_all_retries_fail(
        self, db_params: dict[str, str | int]
    ) -> None:
        """Test connection initialisation when all retries fail."""
        connection = DatabaseConnection(
            db=str(db_params["db"]),
            host=str(db_params["host"]),
            port=int(db_params["port"]),
            timeout=int(db_params["timeout"]),
        )
        error = OperationalError("mock error", None, None)

        with (
            patch("app.db.connect.create_engine", side_effect=error),
            patch("app.db.connect.sleep") as mock_sleep,
            patch.object(
                connection,
                "_get_available_driver",
                return_value="ODBC Driver 17 for SQL Server",
            ),
            patch("pyodbc.drivers", return_value=["ODBC Driver 17 for SQL Server"]),
        ):
            with pytest.raises(DatabaseConnectionError) as exc_info:
                connection._init_connection()

            assert "Failed to connect to database:" in str(exc_info.value)
            assert mock_sleep.call_count == 2  # Two retries with delays

    def test_get_session_success(
        self,
        db_params: dict[str, str | int],
        mock_session_maker: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """Test successful session creation and usage."""
        connection = DatabaseConnection(
            db=str(db_params["db"]),
            host=str(db_params["host"]),
            port=int(db_params["port"]),
            timeout=int(db_params["timeout"]),
        )
        connection._session_maker = mock_session_maker

        with connection.get_session() as session:
            assert session == mock_session

        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()

    def test_get_session_not_initialised(self, db_params: dict[str, str | int]) -> None:
        """Test get_session when connection is not initialised."""
        connection = DatabaseConnection(
            db=str(db_params["db"]),
            host=str(db_params["host"]),
            port=int(db_params["port"]),
            timeout=int(db_params["timeout"]),
        )

        with pytest.raises(DatabaseConnectionError) as exc_info:
            with connection.get_session():
                pass

        assert str(exc_info.value) == "Database connection not initialised."

    def test_get_session_with_error(
        self,
        db_params: dict[str, str | int],
        mock_session_maker: MagicMock,
        mock_session: MagicMock,
    ) -> None:
        """Test session handling when an error occurs."""
        connection = DatabaseConnection(
            db=str(db_params["db"]),
            host=str(db_params["host"]),
            port=int(db_params["port"]),
            timeout=int(db_params["timeout"]),
        )
        connection._session_maker = mock_session_maker
        test_error = ValueError("Test error")

        with pytest.raises(ValueError):
            with connection.get_session():
                raise test_error

        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()

    def test_close(
        self, db_params: dict[str, str | int], mock_engine: MagicMock
    ) -> None:
        """Test connection closure."""
        connection = DatabaseConnection(
            db=str(db_params["db"]),
            host=str(db_params["host"]),
            port=int(db_params["port"]),
            timeout=int(db_params["timeout"]),
        )
        connection._engine = mock_engine

        connection._close()

        mock_engine.dispose.assert_called_once()
        assert connection._engine is None

    def test_get_connection_context_manager(
        self, db_params: dict[str, str | int], mock_engine: MagicMock
    ) -> None:
        """Test the get_connection context manager."""
        with (
            patch("app.db.connect.create_engine", return_value=mock_engine),
            patch("app.db.connect.sessionmaker"),
            patch("pyodbc.drivers", return_value=["ODBC Driver 17 for SQL Server"]),
        ):
            with DatabaseConnection.get_connection(
                db=str(db_params["db"]),
                host=str(db_params["host"]),
                port=int(db_params["port"]),
                timeout=int(db_params["timeout"]),
            ) as connection:
                assert isinstance(connection, DatabaseConnection)
                assert connection._engine == mock_engine

            mock_engine.dispose.assert_called_once()

    def test_get_connection_with_error(
        self, db_params: dict[str, str | int], mock_engine: MagicMock
    ) -> None:
        """Test the get_connection context manager error handling."""
        test_error = ValueError("Test error")

        with (
            patch("app.db.connect.create_engine", return_value=mock_engine),
            patch("app.db.connect.sessionmaker"),
            patch("pyodbc.drivers", return_value=["ODBC Driver 17 for SQL Server"]),
            pytest.raises(ValueError),
        ):
            with DatabaseConnection.get_connection(
                db=str(db_params["db"]),
                host=str(db_params["host"]),
                port=int(db_params["port"]),
                timeout=int(db_params["timeout"]),
            ):
                raise test_error

        mock_engine.dispose.assert_called_once()
