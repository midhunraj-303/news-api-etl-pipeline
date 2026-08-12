#   Clean data.

# Handle missing authors.
# Remove duplicate articles.
# Validate URLs.
# Standardize timestamp.
# Remove invalid records.
# Produce a clean Silver dataset.

from scripts.logger import logger
from scripts.spark import create_spark, stop_spark
from pyspark.sql import DataFrame
from pyspark.sql.functions import(
    col,
    coalesce,
    lit,
    to_timestamp
)

# ------------------- read data from bronze layer --------------
def read_bronze(config: dict):
    """
    Read bronze layer
    args:
        config: Project configuration
    Returns: SparkSession and DataFrame
    """

    logger.info("-" * 60)
    logger.info("READING BRONZE LAYER")
    logger.info("-" * 60)

    spark = create_spark(config)
    bronze_path = config["output"]["bronze_path"]

    df = spark.read.parquet(bronze_path)

    logger.info("Bronze layer loaded successfully")


    return spark, df


# ---------------- cleaning functions----------------------------
#remove duplicates
def remove_duplicate_articles(df: DataFrame) -> DataFrame:
    """
    Remove duplicate articles bases on URL
    Args:
        df: Spark DataFrame
    Returns: DataFrame (Cleaned)
    """
    logger.info("-" * 60)
    logger.info("REMOVING DUPLICATE ARTICLES")
    logger.info("-" * 60)

    cleaned_df = df.dropDuplicates(["url"])

    before_remove = df.count()
    after_remove = cleaned_df.count()

    logger.info("Duplicate articles removed")

    logger.info(f"Total articles        : {before_remove}")
    logger.info(f"Duplicates removed    : {before_remove - after_remove}")

    return cleaned_df


# Replace missing author name with 'Unknown'
def clean_author(df: DataFrame) -> DataFrame:
    """
    Replace missing author names
    Args: 
        df: Spark DataFrame

    Returns: Cleaned DataFrame
    """
    logger.info("-" * 60)
    logger.info("CLEANING AUTHOR COLUMN")
    logger.info("-" * 60)

    cleaned_df = df.withColumn(
        "author",
        coalesce(col("author"), lit("Unknown"))
    )

    count_unknown = cleaned_df.filter(col("author") == "Unknown").count()

    logger.info(f"Unknown authors : {count_unknown}")

    logger.info("Author Cleaning completed")

    return cleaned_df


#Clean description
def clean_description(df: DataFrame) -> DataFrame:
    """
    Replace missing description with 'No Description Available'
    Args:
        df: Spark DataFrame
    Return: Cleaned description
    """
    logger.info("-" * 60)
    logger.info("CLEANING DESCRIPTION COLUMN")
    logger.info("-" * 60)

    df = df.withColumn(
        "description",
        coalesce(col("description"),lit("No Description Available"))
    )

    no_description_count = df.filter(
        col("description") =="No Description Available"
    ).count()

    logger.info(f"News without description  : {no_description_count}")

    logger.info(f"Description cleaning completed")

    return df


#clean content
def clean_content(df: DataFrame) -> DataFrame:
    """
    Replace missing article content with 'No Content Available'
    
    args:
        df: Spark DataFrame
    return: DataFrame
    """
    logger.info("-" * 60)
    logger.info("CLEANING ARTICLE CONTENT")
    logger.info("-" * 60)

    df = df.withColumn(
        "content",
        coalesce(col("content"),lit("No Content Available"))
    )

    no_content_count = df.filter(
        col("content") == "No Content Available"
    ).count()

    logger.info(f"News without content     : {no_content_count}")
    logger.info("Content cleaning completed")

    return df


# convert publishedAt to spark date format
def convert_published_date(df :DataFrame) -> DataFrame:
    """
    Convert publishedAt from string to timestamp
    args:
        df:Spark DataFrame
    Returns: Cleaned DataFrame
    """

    logger.info("-" * 60)
    logger.info("CLEANING PUBLISH DATE")
    logger.info("-" * 60)

    df = df.withColumn(
        "PublishedAt",
        to_timestamp( col("publishedAt"))
    )

    df.printSchema()
    df.select("publishedAt").show(15,truncate=False)
    logger.info("publishedAt conversion completed")

    return df


def validate_urls(df:DataFrame) -> DataFrame:
    """
    Remove records with invalid urls
    Args:
        df: Spark DataFrame
    Returns: Cleaned DataFrame
    """
    logger.info("-" * 60)
    logger.info("VALIDATING ARTICLE URLS")
    logger.info("-" * 60)

    before = df.count()

    df = df.filter(
        col("url").isNotNull()
    ).filter(
        col("url").rlike(r"^https?://")
    )

    after = df.count()

    invalid_count = before - after
    logger.info(f"Invalid URLs  : {invalid_count}")
    logger.info("URL Validation Completed")

    return df


def write_silver(df: DataFrame,config: dict) -> None:
    """
    Write clean data to silver layer
    Args:
        df: Clean DataFrame
        config: Project configuration
    Returns: None
    """

    silver_path = config["output"]["silver_path"]

    logger.info("-" * 60)
    logger.info(f"WRITING SILVER LAYER : {silver_path}")
    logger.info("-" * 60)

    df.write.mode("overwrite").parquet(silver_path)

    logger.info("Silver Layer Written Successfully")



# run cleaned
def run_cleaned(config: dict) -> None:
    """
    Combine all functions
    args:
        config: Project configuration

    returns: None
    """

    logger.info("           START DATA CLEANING")

    spark,df = read_bronze(config)     # This DataFrame call all cleaning functions

    remove_duplicate_articles(df)
    clean_author(df)
    clean_description(df)
    clean_content(df)
    convert_published_date(df)
    validate_urls(df)
    write_silver(df,config)

    stop_spark(spark)
    logger.info("-" * 60)
    logger.info("CLEANING STAGE COMPLETED SUCCESSFULLY")
    logger.info("-" * 60)


