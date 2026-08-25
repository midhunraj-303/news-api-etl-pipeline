# Dimension Table and Fact Table

# Grain : one row represents one news article

from pyspark.sql import SparkSession, DataFrame
from scripts.spark import create_spark,stop_spark
from scripts.logger import logger
from pyspark.sql.functions import (
    col,
    monotonically_increasing_id,
    to_date,
    date_format,
    dayofmonth,
    month,
    quarter,
    year,
    when,
    coalesce,
    lit
)
from scripts.warehouse import load_to_postgres

def read_silver(config: dict) -> tuple[SparkSession,DataFrame]:
    """
    Read the silver layer for Star schema processing
    Args:
        config: project configuration
    returns: SparkSession and DataFrame
    """

    logger.info("-" * 60)
    logger.info("READING SILVER LAYER FOR STAR SCHEMA")
    logger.info("-" * 60)

    spark = create_spark(config)

    silver_path = config["output"]["silver_path"]

    df = spark.read.parquet(silver_path)

    logger.info("Silver layer loaded successfully")

    return spark,df

# ------------------------------ dimention tables---------------------------------

def create_dim_source(df: DataFrame) -> DataFrame:
    """
    Create the source dimension

    Args: 
        df: Silver DataFrame
    Returns: Source dimension DataFrame
    """

    logger.info("-" * 60)
    logger.info("CREATING SOURCE DIMENSIONS")
    logger.info("-" * 60)

    dim_source = (
        df
        .select(
            col("source.id").alias("source_id"),
            col("source.name").alias("source_name")
        )
        .dropDuplicates(["source_name"])
        .withColumn(
            "source_key",
            monotonically_increasing_id()
        )
        .select(
            "source_key",
            "source_id",
            "source_name"
        )
        
    )

    logger.info(f"Created {dim_source.count()} source dimension records")

    return dim_source


def create_dim_author(df: DataFrame) ->DataFrame:
    """
    create the author dimension

    Args:
        df: Silver Dataframe
    Returns:
        Author dimension DataFrame
    """

    logger.info("-" * 60)
    logger.info("CREATING AUTHOR DIMENSION")
    logger.info("-" * 60)

    dim_author = (
        df
        .select(
            col("author").alias("author_name")
        )
        .fillna(
            {"author_name" : "Unknown"}
        )
        .dropDuplicates(["author_name"])
        .withColumn(
            "author_key",
            monotonically_increasing_id()
        )
        .select(
            "author_key",
            "author_name"
        )
    )

    logger.info(f"Created {dim_author.count()} author dimension records")

    return dim_author


# create_dim_date() - allow us to query articles by year, month, quarter, weekday, etc.
# without repeatedly calculating these values from PublishedAt
def create_dim_date(df:DataFrame) -> DataFrame:
    """
    create the date dimension
    Args:
        df: Silver DataDrame
    Returns: Date dimension DataFrame
    """

    logger.info("-" * 60)
    logger.info("CREATING DATE DIMENSION")
    logger.info("-" * 60)

    dim_date = (
        df
        .select(
            to_date(col("PublishedAt")).alias("full_date")
        )
        .dropDuplicates(["full_date"])
        .filter(col("full_date").isNotNull())
        .withColumn(
            "date_key",
            date_format(col("full_date"), "yyyyMMdd").cast("int")
        )
        .withColumn(
            "day",
            dayofmonth(col("full_date"))
        )
        .withColumn(
            "month",
            month(col("full_date"))
        )
        .withColumn(
            "month_name",
            date_format(col("full_date"),"MMMM")
        )
        .withColumn(
            "quarter",
            quarter(col("full_date"))
        )
        .withColumn(
            "year",
            year(col("full_date"))
        )
        .withColumn(
            "weekday",
            date_format(col("full_date"), "EEEE")
        )
        .select(
            "date_key",
            "full_date",
            "day",
            "month",
            "month_name",
            "quarter",
            "year",
            "weekday"
        )
        .orderBy("full_date")
    )

    logger.info(f"Created {dim_date.count()} date dimension records")

    return dim_date

# ------------------------------fact table-------------------------------------------------

def create_fact_articles(
    df: DataFrame,
    dim_source: DataFrame,
    dim_author: DataFrame,
    dim_date: DataFrame    
)->DataFrame:
    """
    create the article fact table

    Grain:
        one row represents one news article
    
    Args:
        df: Silver DataFrame
        dim_source: Source dimension
        dim_author: Author dimension
        dim_date: Date dimension

    Returns:
        Fact articles DataFrame
    """

    logger.info("-" * 60)
    logger.info("CREATING ARTICLE FACT TABLE")
    logger.info("-" * 60)

    article_df = (
        df
        .withColumn(
            "source_name",
            col("source.name")
        )
        .withColumn(
            "published_date",
            to_date(col("PublishedAt"))
        )
        .withColumn(
            "author_name",
            coalesce(col("author"),lit("Unknown"))
        )
    )

    fact_df = (
        article_df

        .join(
            dim_source,
            on="source_name",
            how="left"
        )

        .join(
            dim_author,
            on="author_name",
            how="left"
        )

        .join(
            dim_date,
            article_df.published_date == dim_date.full_date,
            how = "left"
        )

        .withColumn(
            "has_image",
            when(
                col("urlToImage").isNotNull(),
                True
            ).otherwise(False)
        )

        .withColumn(
            "article_key",
            monotonically_increasing_id()
        )

        .select(
            "article_key",
            "source_key",
            "author_key",
            "date_key",
            "title",
            "description",
            "url",
            "urlToImage",
            "PublishedAt",
            "content",
            "has_image"
        )
    )

    logger.info(f"Created {fact_df.count()} fact article records")

    return fact_df


# ------------------------------Validate ---------------------------------------
# Check joins produce the correct foreign keys or not
def validate_fact_articles(fact_df: DataFrame) ->None:
    """
    validate the article fact ttable
    Args:
        fact_df: Fact articles DataFrame
    Returns: None
    """

    logger.info("-" * 60)
    logger.info("VALIDATING ARTICLE FACT TABLE")
    logger.info("-" * 60)

    total_articles = fact_df.count()

    null_source_keys = fact_df.filter(
        col("source_key").isNull()
    ).count()

    null_author_keys = fact_df.filter(
        col("author_key").isNull()
    ).count()

    null_date_keys = fact_df.filter(
        col("date_key").isNull()
    ).count()

    duplicate_articles = (
        fact_df
        .groupBy("url")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    logger.info(f"Total articles    : {total_articles}")
    logger.info(f"Null source keys  : {null_source_keys}")
    logger.info(f"Null author keys  : {null_author_keys}")
    logger.info(f"Null date keys    : {null_date_keys}")
    logger.info(f"Duplicate URLs    : {duplicate_articles}")

    logger.info("Fact table validation completed")


# -----------------------------load star schema to postgres--------------------------
def load_star_schema_to_postgres(
        dim_source: DataFrame,
        dim_author: DataFrame,
        dim_date: DataFrame,
        fact_articles: DataFrame,
        config: dict
) -> None:
    """
    Load star schema tables into postgreSQL

    Load order:
    1. dim_source
    2. dim_author
    3. dim_date
    4. fact_df

    args:
        dim_source: Source dimension
        dim_author: Author dimension
        dim_date: Date dimension
        fact_articles: Article fact table
        config: Project configuration
    Returns: None
    """

    logger.info("-" * 60)
    logger.info("LOADING STAR SCHEMA INTO POSTGRESQL")
    logger.info("-" * 60)

    logger.info(
        f"Fact articles before PostgreSQL load: {fact_articles.count()}"
)

    load_to_postgres(
        df = dim_source,
        table_name = "dim_source",
        config = config
    ) 

    load_to_postgres(
        df = dim_author,
        table_name = "dim_author",
        config = config
    )

    load_to_postgres(
        df = dim_date,
        table_name = "dim_date",
        config = config
    )

    load_to_postgres(
        df = fact_articles,
        table_name = "fact_articles",
        config = config
    )

    logger.info("-" * 60)
    logger.info("STAR SCHEMA LOADED SUCCESSFULLY")
    logger.info("-" * 60)


# ----------------------------- run star schema--------------------------------------
def run_star_schema(config: dict) -> None:
    """
    Run star schema
    Args: 
        config: Project Configuration
    returns: None
    """

    logger.info("-" * 60)
    logger.info("STARTING STAR SCHEMA")
    logger.info("-" * 60)

    spark, df =read_silver(config)

    dim_source =create_dim_source(df)
    # dim_source.show(truncate=False)

    dim_author = create_dim_author(df)
    # dim_author.show(truncate = False)

    dim_date = create_dim_date(df)
    # dim_date.show()

    fact_df = create_fact_articles(
        df,
        dim_source,
        dim_author,
        dim_date,
    )

    # fact_df.show(10, truncate=False)

    validate_fact_articles(fact_df)
    logger.info("Star schema completed successfully")

    load_star_schema_to_postgres(dim_source,dim_author,dim_date,fact_df,config)


    
    stop_spark(spark)