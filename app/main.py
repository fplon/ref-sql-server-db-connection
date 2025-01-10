# Sample usage

from app.db.connect import DatabaseConnection


def main() -> None:
    with DatabaseConnection.get_connection(
        db="sample",
        host="localhost",
        port=1433,
    ) as db:
        with db.get_session() as session:
            result = session.execute("SELECT 1")
            print(result)


if __name__ == "__main__":
    main()
