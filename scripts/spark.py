import os
import sys
from pyspark.sql import SparkSession
from scripts.logger import logger


def create_spark(config: dict) -> SparkSession:
    """
    Create and return a SparkSession

    args:
        config: Project COnfiguration

    returns:
        SparkSession object
    """

    logger.info("Creating SparkSession")

    # Use the current virtual environment's Python executable
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName(config["spark"]["app_name"])
        .master(config["spark"]["master"])
        .getOrCreate()
    )

    logger.info("SparkSession created successfully")

    return spark


# stop_spark()
# useful for
# log execution time,
# clean temporary views,
# clear Spark cache,
# collect Spark metrics.
# Having a dedicated function gives us one place to add those enhancements.

def stop_spark(spark: SparkSession) -> None:
    """
    Stop the SparkSession
    args:
        spark: SparkSession object
    returns: None
    """

    logger.info("Stopping Spark Session")

    spark.stop()

    logger.info("Spark session stopped successfully")
