# Only communicate with NewsAPI.

# HTTP requests
# API authentication
# Pagination
# Retry
# Rate limit handling

import requests
from scripts.logger import logger


# -------------------NewsAPI client configuration----------------------
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

# --------------------------API call---------------------------------
def send_request(config: dict) -> requests.Response:
    """
    send a GET request to the news API
    args:
        config: Project configuration
    Returns: Response object feom NewsAPI
    """

    logger.info("Sending request to NewsAPI")

    response =requests.get(
        url = build_url(config),
        headers = build_headers(config),
        params = build_query_params(config),
        timeout = 30
    )

    logger.info(f"HTTP Status code: {response.status_code}")

    return response


def validate_response(response: requests.Response) -> dict:
    """
    Validate the newsAPI response
    args:
        response: Response object returned by requests
    Returns: parsed JSON response
    Rsises: 
        RuntimeError: If the API request fails
    """

    logger.info("Validating NewsAPI response")

    if response.status_code != 200:
        raise RuntimeError(
            f"NewsAPI request failed with HTTP {response.status_code}"
        )

    data = response.json()

    if data.get("status") != "ok":
        raise RuntimeError(
            f"NewsAPI returned an error: {data.get('message')}"
        )

    logger.info(
        f"Retrieved {len(data.get('articles', []))} articles"
    )

    return data


def fetch_news(config: dict) -> dict:
    # This is the only function the rest of the project should use
    """
    Fetch news articles from NewsAPI

    args:
        config: Project configuration

    returns: Parsed newsAPI response

    """

    logger.info("Fetching news articles")

    response = send_request(config)

    data = validate_response(response)

    logger.info("News articles fetched successfully")
    
    return data