# SQL Server Database Connection Utility

Python utility for managing SQL Server database connections using SQLAlchemy and PyODBC. 

## Features

- Context-managed database connections
- Automatic ODBC driver detection
- Connection retry mechanism with exponential backoff
- Support for multiple SQL Server ODBC drivers
- Session management with automatic cleanup
- Transaction handling with automatic rollback on failure

## Prerequisites

- Python 3.13.0 (Didn't test but should work 3.9+)
- SQL Server ODBC Driver (one of the following):
  - ODBC Driver 17 for SQL Server
  - ODBC Driver 13 for SQL Server
  - SQL Server Native Client 11.0

## Installation

1. Install the required packages:
```bash
pip install -r requirements.txt
```

## Usage

Here's a basic example of how to use the database connection utility:

```python
from app.db import DatabaseConnection

# Connect to the database
with DatabaseConnection.get_connection(
    db='your_database',
    host='your_host',
    port=1433,
    timeout=30
) as db:
    # Get a session for database operations
    with db.get_session() as session:
        # Perform your database operations here
        pass
```

## Error Handling

The utility includes built-in error handling for common database connection issues:
- Automatic retry on connection failures
- Transaction rollback on errors
- Proper connection cleanup
- Detailed error messages and logging

## Configuration

The connection utility supports the following parameters:
- `db`: Database name
- `host`: Database server hostname
- `port`: Database server port (default: 1433)
- `timeout`: Connection timeout in seconds (default: 30)

Note: Set up for Windows auth but could be tailored to suit. 

## License

MIT
