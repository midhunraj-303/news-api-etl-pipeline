# Analyze data quality.

# Missing values
# Duplicate URLs
# Language distribution
# Source distribution
# Null counts

from scripts.logger import logger
from pyspark.sql import DataFrame
from scripts.spark import create_spark
from pyspark.sql.types import StructType
from pyspark.sql.functions import (
    col,
    count,
    when,
    desc
)
from scripts.spark import stop_spark


#--------------Read data from bronze layer-------------------
def read_bronze(config: dict):
    """
    Read Bronze layer.
    Args:
        config: Project configuration

    Returns:
        SparkSession and Bronze DataFrame
    """

    logger.info("reading Bronze layer")

    spark = create_spark(config)

    bronze_path = config["output"]["bronze_path"]

    df = spark.read.parquet(bronze_path)

    logger.info("Bronze layer loaded succesfully")

    return spark,df
    # return spark because it created its own SparkSession, 
    # it is responsible for stopping SparkSession
    # in run_profile() we use stop_spark(spark)  


# ------------------------- Start Data profiling ------------------------------------

def count_records(df: DataFrame) -> int:
    """
    Count the number of records
    Args:
        df: Spark DataFrame

    Returns: Number of records
    """

    logger.info("Counting records")

    total_records = df.count()

    logger.info(f"Total Records : {total_records}")

    return total_records


def count_columns(df: DataFrame) -> int:
    """
    count the number of columns

    Args:
        df: Spark DataFrame
    Returns:
        Number of columns
    """

    logger.info("Counting columns")

    total_columns = len(df.columns)

    logger.info(f"Total columns: {total_columns}")

    return total_columns


def summarize_schema(df: DataFrame) -> None:
    """
    Display the DataFrame schema

    args:
        df: Spark DataFrame

    Returns: None
    """

    logger.info("Schema Summary")

    logger.info("=" * 60)
    logger.info(f"{'Column':25} {'Data Type':25} {'Nullable'}")
    logger.info("=" * 60)

    for field in df.schema.fields:
        logger.info(
            f"{field.name:25} "
            f"{field.dataType.simpleString():20}"
            f"{field.nullable}"
        )
    logger.info("=" * 60)


# count null values
def count_null_values(df: DataFrame) -> None:
    """
    Count null values for each column
    args:
        df: Spark DataFrame
    Returns: None
    """

    logger.info("Null value analysis")

    null_counts = df.select([
        count(
            when(col(column).isNull(), column)
        ).alias(column)
        for column in df.columns
    ])

    null_counts.show(truncate = False)


# Count duplicate urls
def count_duplicate_urls(df: DataFrame) -> int:
    """
    Count duplicate urls
    args:
        df: Spark DataFrame
    returns: Number of duplicate urls
    """

    logger.info("Checking Duplicate urls")

    total_records = df.count()

    distinct_urls = df.select("url").distinct().count()

    duplicate_urls = total_records - distinct_urls

    logger.info(f"Total records : {total_records}")
    logger.info(f"Distinct URLs : {distinct_urls}")
    logger.info(f"Duplicate urls : {duplicate_urls}" )

    return duplicate_urls


# Display distribution of news sources
# Which news sources contribute the most articles?
def source_distribution(df: DataFrame) -> None:
    """
    Display the distribution of news sources
    args:
        df: Spark DataFrame
    returns: None
    """ 

    logger.info("News Source Distribution")

    (
        df
        .groupBy(col("source.name"))
        .count()
        .orderBy(desc("count"))
        .show(truncate = False)
    )


# create a custom descriptive summary
# total articles
# Unique sources
# articles with author
# articles without author
# articles with image
# articles without image
def descriptiove_statistics(df: DataFrame) -> None:
    """
    Display discriptive statistics
    Args:
        df: Spark DataFrame
    Returns: None
    """

    logger.info("Discriptive Statistics")

    total_articles = df.count()

    unique_sources = df.select("source.name").distinct().count()

    articles_with_author = (
        df.filter(col("author").isNotNull())
        .count()
    )

    articles_without_author = (
        df.filter(col("author").isNull())
        .count()
    )

    articles_with_image = (
        df.filter(col("urlToImage").isNotNull())
        .count()
    )

    articles_without_image = (
        df.filter(col("urlToImage").isNull())
        .count()
    )


    logger.info(f"Total articles            : {total_articles}")
    logger.info(f"Unique sources            : {unique_sources}")
    logger.info(f"Articles with author      : {articles_with_author}")
    logger.info(f"Articles without author   : {articles_without_author}")
    logger.info(f"Articles with image       : {articles_with_image}")
    logger.info(f"Articles without image    : {articles_without_image}")

def run_profile(config: dict) -> None:
    """
    Execute the complete profiling stage
    Args:
        config: Project configuration
    Returns : None
    """

    logger.info("Starting profiling stage")

    spark,df = read_bronze(config)

    count_records(df)

    summarize_schema(df)

    count_null_values(df)

    count_duplicate_urls(df)

    source_distribution(df)

    descriptiove_statistics(df)

    stop_spark(spark)

    logger.info("Profiling stage completed successfully")