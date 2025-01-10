from contextlib import contextmanager
from time import sleep
from typing import Generator

import pyodbc
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from .exceptions import DatabaseConnectionError


class DatabaseConnection:
    """
    Manage connection and session to SQL Server.
    Construction via get_connection() factory method.
    """

    SUPPORTED_DRIVERS = frozenset(
        {
            # Amend as needed or move to config/env
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 13 for SQL Server",
            "SQL Server Native Client 11.0",
        }
    )

    def __init__(self, db: str, host: str, port: int, timeout: int = 30) -> None:
        self._db = db
        self._host = host
        self._port = port
        self._timeout = timeout

        self._engine: Engine | None = None
        self._session_maker: sessionmaker | None = None

    def _get_available_driver(self) -> str:
        """
        Get first available driver for the SQL Server.
        """
        available_drivers = self.SUPPORTED_DRIVERS.intersection(pyodbc.drivers())
        if not available_drivers:
            raise DatabaseConnectionError("No supported ODBC driver found.")
        return next(iter(available_drivers))

    def _init_connection(self) -> None:
        """
        Initialise connection with retries.
        """
        _max_retries = 3
        _init_delay = 1
        _backoff_factor = 2

        for attempt in range(_max_retries):
            try:
                driver = self._get_available_driver()
                connection_string = (
                    f"mssql+pyodbc://{self._host}:{self._port}/{self._db}?"
                    f"driver={driver}&trusted_connection=yes"
                )

                self._engine = create_engine(
                    connection_string,
                    fast_executemany=True,
                    connect_args={"timeout": self._timeout},
                )
                self._session_maker = sessionmaker(bind=self._engine)

                # Test connection - fail early
                with self._engine.connect():
                    print(
                        f"INFO: Connection to database successful. Host: {self._host}, Port: {self._port}, Database: {self._db}"
                    )

                return

            except (OperationalError, SQLAlchemyError) as e:
                if attempt < _max_retries - 1:
                    delay = _init_delay * (_backoff_factor**attempt)
                    print(
                        f"WARNING: Connection attempt {attempt + 1} failed. Retrying in {delay} seconds"
                    )
                    sleep(delay)
                else:
                    print(
                        f"ERROR: Failed to connect to database after {_max_retries} attempts."
                    )
                    raise DatabaseConnectionError(
                        f"Failed to connect to database: {str(e)}"
                    ) from e

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Get a new session from the session maker.
        """
        if not self._session_maker:
            raise DatabaseConnectionError("Database connection not initialised.")

        session = self._session_maker()

        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"ERROR: Database session failed to commit: {str(e)}")
            raise
        finally:
            session.close()

    def _close(self) -> None:
        """
        Close the database connection and session maker.
        """
        if self._engine:
            self._engine.dispose()
            self._engine = None
            print("INFO: Database connection closed.")

    @classmethod
    @contextmanager
    def get_connection(
        cls, db: str, host: str, port: int, timeout: int = 30
    ) -> Generator["DatabaseConnection", None, None]:
        """
        Connect to the database and yield a DatabaseConnection instance.

        Example:
            with DatabaseConnection.get_connection(db='mydb', host='localhost', port=1433, timeout=30) as db:
                with db.get_session() as session:
                    # Use session to perform database operations
                    pass
        """
        connection = cls(db, host, port, timeout)
        connection._init_connection()
        try:
            yield connection
        except Exception as e:
            print(
                f"ERROR: Unexpected error occurred while connecting to database: {str(e)}"
            )
            raise
        finally:
            connection._close()
