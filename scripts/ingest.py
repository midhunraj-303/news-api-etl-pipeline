# first stage of the ETL pipeline.
# Retrieve data from NewsAPI and store it in the Bronze layer.

# Create the Bronze layer.
# Call news_api.py
# Save raw JSON
# Convert JSON to Spark
# Store Bronze Parquet
# ------------------------------------------------------------------

from pyspark.sql import SparkSession
from scripts.config import load_config
from scripts.logger import logger
from scripts.news_api import fetch_news
from scripts.schema import NEWS_ARTICLE_SCHEMA
from scripts.spark import create_spark 

def create_dataframe(
        spark: SparkSession,
        articles: list
):
    """
    Create a Spark DataFrame from NewsAPI articles.

    args:
        spark: SparkSession object.
        articles: List of news articles
    
    Returns: Spark DataFrame
    """

    logger.info("Creating Spark DataFrame")

    df = spark.createDataFrame(
        articles,
        schema = NEWS_ARTICLE_SCHEMA
    )

    logger.info(f"DataFrame created with {df.count()} records")

    return df
    

def load_news(config: dict) -> dict:
    """
    Load news data from NewsApi
    args:
        config: project configuration
    
    return: News api response as dictionary
            result -> 
                news_data ={
                    'status' : 'ok'
                    'totalResults' : int
                    'articles' : list of dict
                }
    """

    logger.info("Loading news data from NewsAPI")

    news_data = fetch_news(config)

    logger.info("News data loaded successfully")

    return news_data


def extract_articles(news_data: dict) -> list:
    """
    Extract 'articles' from the NewsAPI response

    args:
        news_data : parsed NewsAPI response

    Returns: List of news articles
    """

    logger.info("Extracting articles from NewsAPI response")

    articles = news_data.get("articles", [])

    logger.info(f"Extracted {len(articles)} articles")

    return articles


def write_bronze(
        df,
        config: dict
) -> None:
    """
    Write the DataFrame to Bronze Layer
    Args:
        df: Spark DataFame
        config: Project configuration
    """

    bronze_path = config["output"]["bronze_path"]

    logger.info(f"Writing Bronze layer to: {bronze_path}")

    (
        df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(bronze_path)
    )

    logger.info("Bronze layer written successfully")


def run_ingestion(config: dict) -> None:
    """
    Execute the complete ingestion pipeline

    args:
        config: Project Configuration
    """


    logger.info("Starting ingestion stage")

    spark = create_spark(config)

    news_data = load_news(config)

    articles = extract_articles(news_data)

    df = create_dataframe(
        spark,
        articles
    )

    write_bronze(
        df,
        config
        )

    spark.stop()

    logger.info("Ingestion stage completed successfully")

