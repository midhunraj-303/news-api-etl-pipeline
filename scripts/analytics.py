# Query the Star Schema to answer business questions
# 1. Articles by source
# 2. Articles by author
# 3. Articles by date
# 4. Articles with/without images
# 5. Top sources
# 6. Top authors
# 7. Daily article trends


from scripts.logger import logger
from scripts.postgres import get_postgres_connection

def articles_by_source(config: dict):
    """
    Get article by news source
    Args:
        config: Project configuration
    Returns:
        Query results
    """

    logger.info("-" * 60)
    logger.info("ANALYTICS: ARTICLES BY SOURCE")
    logger.info("-" * 60)

    query = """
            SELECT
                s.source_name,
                COUNT(*) AS article_count
            FROM news_dw.fact_articles f
            JOIN news_dw.dim_source s
                ON f.source_key = s.source_key
            GROUP BY s.source_name
            ORDER BY article_count DESC;
            """

    conn = get_postgres_connection(config)

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)

            results = cursor.fetchall()

            for row in results:
                logger.info(
                    f"Source: {row[0]} | Articles: {row[1]}"
                )

            return results

    except Exception:
        logger.info(f"Error while getting articles by source")

    finally:
        conn.close()


def articles_by_date(config: dict):
    """
    Get article count by publication date
    Args:
        config: Project Configuration
    Returns:
        Query results
    """

    logger.info("-" * 60)
    logger.info("ANALYTICS: ARTICLES BY DATE")
    logger.info("-" * 60)

    query = """
            SELECT
                d.full_date,
                COUNT(*) AS article_count
            FROM news_dw.fact_articles f
            JOIN news_dw.dim_date d
                ON f.date_key = d.date_key
            GROUP BY d.full_date
            ORDER BY d.full_date;

            """
    conn = get_postgres_connection(config)

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)

            results = cursor.fetchall()

            for row in results:
                logger.info(
                    f"Date: {row[0]} | Articles: {row[1]}"
                )

            return results

    except Exception:
        logger.info("Error while getting articles by date")

    finally:
        conn.close()

def articles_by_author(config: dict):
    """
    Get article count by author
    Args:
        config: Project Configuration
    Returns:
        Query results
    """

    logger.info("-" * 60)
    logger.info("ANALYTICS: ARTICLES BY AUTHOR")
    logger.info("-" * 60)

    query = """
            SELECT
                a.author_name,
                COUNT(*) AS article_count
            FROM news_dw.fact_articles f
            JOIN news_dw.dim_author a
                ON  f.author_key = a.author_key
            GROUP BY a.author_name
            ORDER BY article_count DESC;
            """
    conn = get_postgres_connection(config)

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)

            results = cursor.fetchall()

            for row in results:
                logger.info(
                    f"Author: {row[0]} | Articles: {row[1]}"
                )

            return results

    except Exception:
        logger.info("Error while getting articles by author")

    finally:
        conn.close()


# How many articles did each news source publish on each date?
def articles_by_source_and_date(config: dict):
    """
    Get articles by source and date
    Args:
        config: Project Configuration
    Returns:
        Query results
    """

    logger.info("-" * 60)
    logger.info("ANALYTICS: ARTICLES BY SOURCE AND DATE")
    logger.info("-" * 60)

    query = """
        SELECT
            s.source_name,
            d.full_date,
            COUNT(*) AS article_count
        FROM news_dw.fact_articles f
        JOIN news_dw.dim_source s
            ON f.source_key = s.source_key
        JOIN news_dw.dim_date d
            ON f.date_key = d.date_key
        GROUP BY
            s.source_name,
            d.full_date
        ORDER BY
            d.full_date,
            article_count DESC;
        """
    conn = get_postgres_connection(config)

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)

            results = cursor.fetchall()

            for row in results:
                logger.info(
                    f"Source: {row[0]} | Date: {row[1]} | Articles: {row[2]}"
                )

            return results

    except Exception:
        logger.info("Error while getting articles by author")

    finally:
        conn.close()


# How many articles did each author publish on each date?
def articles_by_author_and_date(config: dict):
    """
    Get article count by author and date

    Args:
        config: Project Configuration

    Returns:
        Query results
    """

    logger.info("-" * 60)
    logger.info("ANALYTICS: ARTICLES BY AUTHOR AND DATE")
    logger.info("-" * 60)

    query = """
            SELECT
                a.author_name,
                d.full_date,
                COUNT(*) AS article_count
            FROM news_dw.fact_articles f
            JOIN news_dw.dim_author a
                ON f.author_key = a.author_key
            JOIN news_dw.dim_date d
                ON f.date_key = d.date_key
            GROUP BY
                a.author_name,
                d.full_date
            ORDER BY
                d.full_date,
                article_count DESC;
            """

    conn = get_postgres_connection(config)

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)

            results = cursor.fetchall()

            for row in results:
                logger.info(
                    f"Author: {row[0]} | Date: {row[1]} | Articles: {row[2]}"
                )

            return results

    except Exception:
        logger.exception(
            "Error while getting articles by author and date"
        )

    finally:
        conn.close()

# How many articles did each author publish for each source on each date?
def articles_by_source_author_and_date(config: dict):
    """
    Get article count by source, author, and date

    Args:
        config: Project Configuration

    Returns:
        Query results
    """

    logger.info("-" * 60)
    logger.info("ANALYTICS: ARTICLES BY SOURCE, AUTHOR AND DATE")
    logger.info("-" * 60)

    query = """
            SELECT
                s.source_name,
                a.author_name,
                d.full_date,
                COUNT(*) AS article_count
            FROM news_dw.fact_articles f
            JOIN news_dw.dim_source s
                ON f.source_key = s.source_key
            JOIN news_dw.dim_author a
                ON f.author_key = a.author_key
            JOIN news_dw.dim_date d
                ON f.date_key = d.date_key
            GROUP BY
                s.source_name,
                a.author_name,
                d.full_date
            ORDER BY
                d.full_date,
                article_count DESC;
            """

    conn = get_postgres_connection(config)

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)

            results = cursor.fetchall()

            for row in results:
                logger.info(
                    f"Source: {row[0]} | "
                    f"Author: {row[1]} | "
                    f"Date: {row[2]} | "
                    f"Articles: {row[3]}"
                )

            return results

    except Exception:
        logger.exception(
            "Error while getting articles by source, author and date"
        )

    finally:
        conn.close()
# ---------------------------------run analytics----------------------------
def run_analytics(config: dict) -> None:
    """
    Run analytics
    Args: 
        config: Project Configuration
    Returns: None
    """

    logger.info("-" * 60)
    logger.info("STARTING DATA ANALYTICS")
    logger.info("-" * 60)

    articles_by_source(config)
    articles_by_date(config)
    articles_by_author(config)
    articles_by_source_and_date(config)
    articles_by_author_and_date(config)
    articles_by_source_author_and_date(config)
    
    logger.info("-" * 60)
    logger.info("DATA ANALYTICS COMPLETED SUCCESSFULY")
    logger.info("-" * 60)