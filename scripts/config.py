# Read YAML configuration files

# Read YAML
# Return Python dictionary
# Validate configuration (later)

import yaml
from pathlib import Path
import os

def load_config(config_path: str) -> dict:
    """
    Load configuration from a YAML file
    args -> config_path: path to the YAML configuration file
    returns: configuration as a dictionary
    """

    config_file = Path(config_path)

    if not config_file.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}"
        )

    with open(config_file,"r",encoding="utf-8") as file:
        config= yaml.safe_load(file)        # safe_load() only loads standard YAML data types 
                                            # and avoids executing arbitrary Python objects from the file.
    # Load NewsAPI key from environment variable
    api_key = os.getenv("NEWSAPI_API_KEY")

    if api_key:
        config["newsapi"]["api_key"] = api_key

    return config