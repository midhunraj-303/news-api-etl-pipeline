from scripts.config import load_config
from scripts.ingest import run_ingestion

config = load_config("config/config_local.yaml")

run_ingestion(config)