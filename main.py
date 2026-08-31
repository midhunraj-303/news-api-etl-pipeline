from scripts.config import load_config
from scripts.ingest import run_ingestion
from scripts.profile import run_profile
from scripts.logger import logger
from scripts.spark import stop_spark
from scripts.profile import read_bronze
from scripts.clean import run_clean
from scripts.transform import run_transform
from scripts.star_schema import run_star_schema

from scripts.warehouse import run_warehouse
from scripts.analytics import run_analytics

config = load_config("config/config_local.yaml")

run_ingestion(config)

run_profile(config)

run_clean(config)

run_transform(config)

run_warehouse(config)

run_star_schema(config)

run_analytics(config)