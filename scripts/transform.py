# Create business-ready data.

# read__silver()            -> Read silver layer 
# articles_by_source()      -> aggregate articles by source
# articles_by_date()        -> aggregate articles by publication date
# articles_by_author()      -> aggregate articles by author
# articles_with_images()    -> Categorize articles based on image category
# summary_metrics()         -> Generate summary metrices for the gold layer
# write_gold()              -> Write gold dataset
# run_transform()           -> run transform methods

from scripts.spark import stop_spark,create_spark
from pyspark.sql import DataFrame
from scripts.logger import logger
from pyspark.sql.functions import(
    col,
    count,
    desc,
    to_date,
    when,
    countDistinct
)


# -----------------------Read silver--------------------------------
def read_silver(config: dict):
    """
    Read silver layer
    args:
        config: Project configuration
    returns: SparkSession and DataFrame
    """

    logger.info("-" * 60)
    logger.info("READING SILVER LAYER")
    logger.info("-" * 60)

    spark = create_spark(config)

    silver_path = config["output"]["silver_path"]

    df = spark.read.parquet(silver_path)

    logger.info("Silver layer loaded successfully")
    return spark,df

# -----------------------------Gold Dataset-------------------------------------

def articles_by_source(df: DataFrame) -> DataFrame:
    """
    aggregate articles by source
    args:
        df: DataFrame
    Returns: Aggregated DataFrame
    """

    logger.info("-" * 60)
    logger.info("CREATING ARTICLE BY SOURCE")
    logger.info("-" * 60)

    source_df = (
        df
        .groupBy("source.name")
        .agg(
            count("*").alias("articles_count")
        )
        .orderBy(desc("articles_count"))
    )

    logger.info(f"Found {source_df.count()} unique news sources ")

    source_df.show(truncate=False)

    logger.info("Article by source created successfully")

    return source_df


def articles_by_date(df: DataFrame) -> DataFrame:
    """
    aggregate articles by publication date.
    Args:
        df: DataFrame
    Returns: Aggregated Dateframe
    """

    logger.info("-" * 60)
    logger.info("CREATING ARTICLES BY PUBLICATION DATE")
    logger.info("-" * 60)

    date_df = (
        df
        .withColumn(
            "published_date",
            to_date(col("publishedAt"))
        )
        .groupBy("published_date")
        .agg(
            count("*").alias("articles_count")
        )
        .orderBy(desc("published_date"))
    )

    logger.info(f"Found {date_df.count()} publication dates")

    logger.info("Articles by publication date created successfully")

    return date_df
    

def articles_by_author(df: DataFrame) -> DataFrame:
    """
    aggregate articles by author
    Args:
        df: silver DataFrame
    Returns: Aggregated DataFrame
    """

    logger.info("-" * 60)
    logger.info("CREATING ARTICLES BY AUTHOR")
    logger.info("-" * 60)

    author_df = (
        df
        .groupBy("author")
        .agg(count("*").alias("articles_count"))
        .orderBy(desc("articles_count"))
        )

    logger.info(f"Found {author_df.count()} unique authors")

    logger.info("Articles by author created successfully")

    return author_df


def articles_with_images(df: DataFrame) -> DataFrame:
    """
    Categorize articles based on image category
    Args:
        df: Silver Dataframe
    Returns: Aggregated DataFrame
    """

    logger.info("-" * 60)
    logger.info("CREATING IMAGE AVAILABILITY REPORT")
    logger.info("-" * 60)

    image_df =( 
    df
    .withColumn(
        "image_status",
        when(
            col("urlToImage").isNull(), "Without Image"
        ).otherwise(
            "With Image"
        )     
    )
    .groupBy("image_status")
    .agg(
        count("*").alias("articles_count")
    )
    .orderBy(desc("articles_count"))
    )

    logger.info(f"Image categories : {image_df.count()}")

    logger.info("Image availability report created successfully")

    return image_df


def summary_metrics(df: DataFrame) -> DataFrame:
    """
    Generate summary metrics for the gold layer

    args:
        df: Silver DataFrame
    returns: DataFrame (Summary statistics)
    """

    logger.info("-" * 60)
    logger.info("CREATING SUMMARY METRICS")
    logger.info("-" * 60)

    metrics_df = df.agg(
        count("*").alias("total_articles"),

        countDistinct("source.name").alias("unique_sources"),

        countDistinct("author").alias("unique_authors"),

        count(
            when(
                col("urlToImage").isNotNull(), True
            )
        ).alias("articles_with_images"),

        count(
            when(
                col("urlToImage").isNull(), True
            )
        ).alias("articles_without_images")
    ) 

    logger.info("Summary matrices generated successfully")

    return metrics_df

def write_gold(
        source_df: DataFrame,
        date_df: DataFrame,
        author_df: DataFrame,
        image_df: DataFrame,
        metrics_df: DataFrame,
        config: dict
) -> None:
    """
    Write gold dataset
    Args:
        source_df: Article by source
        date_df: Article by publication date
        author_df: Article by author
        image_df: articles with/without image
        metrics_df: Summary metrics
        config: Project configuration
    Returns: None
    """

    logger.info("-" * 60)
    logger.info("WRITING GOLD LAYER")
    logger.info("-" * 60)

    gold = config["output"]["gold"]

    source_df.write.mode("overwrite").parquet(gold["articles_by_source"])

    date_df.write.mode("overwrite").parquet(gold["articles_by_date"])

    author_df.write.mode("overwrite").parquet(gold["articles_by_author"])

    image_df.write.mode("overwrite").parquet(gold["articles_with_images"])

    metrics_df.write.mode("overwrite").parquet(gold["summary_metrics"])

    logger.info("All GOld dataset written successfully")
    

# -------------------------Run-----------------------------------------
def run_transform(config: dict) -> None:
    """
    run transform methods
    Args:
        config: Project configuration
    Returns: None
    """

    logger.info("-" * 60)
    logger.info("STARTING TRANSFORMATION STAGE")
    logger.info("-" * 60)
    
    spark, df = read_silver(config)

    # df.show(5, truncate=False)
    source_df = articles_by_source(df)
    date_df = articles_by_date(df)
    author_df = articles_by_author(df)
    image_df = articles_with_images(df)
    metrics_df = summary_metrics(df)
    
    write_gold(
        source_df,
        date_df,
        author_df,
        image_df,
        metrics_df,
        config
        )

    stop_spark(spark)

    logger.info("-" * 60)
    logger.info("TRANSFORMATION STAGE COMPLETED SUCCESSFULLY")
    logger.info("-" * 60)

