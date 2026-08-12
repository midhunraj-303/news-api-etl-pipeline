from scripts.config import load_config
from scripts.ingest import run_ingestion
from scripts.profile import run_profile
from scripts.logger import logger
from scripts.spark import stop_spark
from scripts.profile import read_bronze

config = load_config("config/config_local.yaml")

run_ingestion(config)

run_profile(config)


