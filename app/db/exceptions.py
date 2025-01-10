class DatabaseConnectionError(Exception):
    """Exception raised for database connection errors."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)
