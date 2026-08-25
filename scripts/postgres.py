import psycopg2
from scripts.logger import logger

def get_postgres_connection(config: dict):
    """
    Create and return a postgreSQL connection

    Args:
        config: Project configuration
    Returns:
        PostgreSQL connection
    """

    logger.info("Connecting to PostgreSQL")

    postgres = config["postgres"]

    connection = psycopg2.connect(
        host = postgres["host"],
        port = postgres["port"],
        database = postgres["database"],
        user = postgres["user"],
        password = postgres["password"]
    )

    logger.info("PostgreSQL Connection established successfully")

    return connection