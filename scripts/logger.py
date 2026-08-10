# Create one common logger for the whole project.

# Configure log format
# Write logs to file
# Print logs to console

import logging
from pathlib import Path
from scripts.config import load_config

def setup_logger() -> logging.Logger:
    """
    Configure and return the project logger
    """

    config = load_config("config/config_local.yaml")

    log_file = config["logging"]["log_file"]
    log_level = config["logging"]["level"]

    # create log directory if it doesn't exists
    # it works regardless of the folder name
    # it doesn't fail if the directory already exists.
    Path(log_file).parent.mkdir(parents=True,exist_ok=True)

    logger = logging.getLogger("news_etl")

    #prevent duplicate handlers
    if logger.hasHandlers():
        return logger

    logger.setLevel(getattr(logging,log_level.upper()))

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
         datefmt="%Y-%m-%d %H:%M:%S"
    )

    #console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    #File Handler
    file_handler =logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

logger = setup_logger()