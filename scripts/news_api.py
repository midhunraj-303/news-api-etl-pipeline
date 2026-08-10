# Only communicate with NewsAPI.

# HTTP requests
# API authentication
# Pagination
# Retry
# Rate limit handling

import requests
from scripts.logger import logger

def build_url(config: dict) -> str:
    """
    Return the newsAPI endpoint URL
    args:
        config: project configuration dictionary
    returns: newsAPI endpoint URL
    """

    logger.info("Building NewsAPI URL")
    return config["newsapi"]["base_url"]


def build_headers(config: dict) -> dict:
    """
    Build HTTP request headers
    args:
        config: project configuration dictionary
    
    returns: Dictionary containing HTTP headers
    """

    logger.info("Building request headers")

    return{
        "X-Api-Key" : config["newsapi"]["api_key"]
    }


def build_query_params(config: dict) -> dict:
    """
    Build query parameters for the NewsAPI request
    args:
        config: Project configuration dictionary
    
    Returns: Dictionary containing query parameters
    """

    logger.info("Building query parameters")

    news_config = config["newsapi"]

    return {
        "q" : news_config["query"],
        "language" : news_config["language"],
        "pageSize" : news_config["page_size"]
    }


