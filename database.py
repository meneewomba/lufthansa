import mysql.connector
import logging
from contextlib import contextmanager

class DatabaseConfig:
    """Database configuration class to store connection parameters."""
    def __init__(self, host='34.163.20.64', database='lufthansa', user='root', password='Andbysw33timean!', port=3306, charset='utf8mb4'):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.charset = charset

    def get_connection(self):
        """Create and return a connection based on the config."""
        return mysql.connector.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port,
            charset=self.charset
        )


# Instantiate the DatabaseConfig
db_config = DatabaseConfig()

@contextmanager
def get_db():
    """Context manager for FastAPI to handle db connections and cursors."""
    try:
        connection = db_config.get_connection()  # Get connection from DatabaseConfig
        
        cursor = connection.cursor(dictionary=True, buffered=True)
        yield cursor  # Return the cursor for the API route
    except mysql.connector.Error as e:
        logging.error(f"Database connection error: {e}")
        yield None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def get_db_persistent():
    """Create a persistent connection for database setup."""
    connection = db_config.get_connection()  # Get connection from DatabaseConfig
    cursor = connection.cursor(dictionary=False, buffered=True)
    return cursor, connection
