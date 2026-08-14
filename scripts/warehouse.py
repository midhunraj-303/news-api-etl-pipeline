# Load gold to PostgreSQL

from typing import Tuple
from pyspark.sql import DataFrame,SparkSession
from scripts.spark import create_spark,stop_spark
from scripts.logger import logger


def read_gold(
        spark: SparkSession,
        config: dict,
        dataset_name: str
) -> Tuple[SparkSession,DataFrame]:
    """
    Read Gold dataset
    Args:
        config: Project configuration
        dataset_name: Name og gold dataset
    Returns: SparkSession and DataFrame
    """

    logger.info("-" * 60)
    logger.info(f"READING GOLD DATASET : {dataset_name}")
    logger.info("-" * 60)

    gold_path = config["output"]["gold"][dataset_name]

    df = spark.read.parquet(gold_path)

    logger.info(f"{dataset_name} loaded successfully")

    return spark,df


def load_to_postgres(
        df: DataFrame,
        table_name: str,
        config: dict
)->None:
    """
    Load a Spark DataFrame into PostgreSQL

    Args:
        df: Spark DataFrame
        table_name: PostgreSQL table name
        config: Project Configuration
    Returns: None
    """

    logger.info("-" * 60)
    logger.info(f"LOADING DATA INTO TABLE : {table_name}")
    logger.info("-" * 60)

    postgres = config["postgres"]

    # jdbc_url format -> jdbc:postgresql://host_name:port_number/database_name
    jdbc_url = (
        f"jdbc:postgresql://"
        f"{postgres['host']}:"
        f"{postgres['port']}/"
        f"{postgres['database']}"
    )

    # Write data to PostgreSQL tables
    (   
        df.write
        .format("jdbc")                             # writing through JDBC
        .option("url",jdbc_url)                     # Database connection url
        .option(
            "dbtable",
            f"{postgres['schema']}.{table_name}"    # Target table
        )
        .option("user", postgres["user"])           # user
        .option("password", postgres["password"])   # password
        .option("driver", "org.postgresql.Driver")  # JDBC driver (jars)
        .mode("overwrite")                          # mode
        .save()
    )

    logger.info(f"{table_name} loaded successfully")


#------------------------ run functions-----------------------------------


def run_warehouse(config: dict) -> None:
    """
    Load all Gold dataset into PostgreSQL

    Args: 
        config: Project configuration
    Returns: None
    """

    logger.info("-" * 60)
    logger.info("STARTING WAREHOUSE LOADING")
    logger.info("-" * 60)

    spark = create_spark(config)
    datasets = [
        "articles_by_source",
        "articles_by_date",
        "articles_by_author",
        "articles_with_images",
        "summary_metrics"
    ]

    for dataset in datasets:

        spark,df =read_gold(spark,config,dataset)

        load_to_postgres(
            df = df,
            table_name=dataset,
            config=config
        )


    stop_spark(spark)

    logger.info("-" * 60)
    logger.info("WAREHOUSE LOADING COMPLETED SUCCESSFULLY")
    logger.info("-" * 60)


