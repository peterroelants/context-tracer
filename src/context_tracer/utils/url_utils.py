from functools import reduce
from urllib.parse import parse_qs, quote, urlencode, urlparse


def join_slash(a: str, b: str) -> str:
    """Join 2 path strings so they have only a single slash between them."""
    return f"{a.rstrip('/')}/{b.lstrip('/')}"


def urljoin_forward_slash(*args: str) -> str:
    """
    Join all arguments to have only a single forward slash (/) between them.
    """
    return reduce(join_slash, args) if args else ""


def parse_url_query_safe(url: str) -> dict[str, str | list[str]]:
    """
    Safely parse URL query parameters and return as dictionary.

    If a parameters is repeated the values will be in a list, otherwise they will be a string value.
    """
    parsed_url = urlparse(url)
    query_components_dct_lists = parse_qs(parsed_url.query, strict_parsing=True)
    return_dct: dict[str, str | list[str]] = {
        k: v[0] if len(v) == 1 else v for k, v in query_components_dct_lists.items()
    }
    return return_dct


def create_query_url(url: str, params: dict) -> str:
    """
    Create an URL with the params translated into a query string.
    The resulting URL should be ready to send with a `application/x-www-form-urlencoded` header.

    More info:
    - https://en.wikipedia.org/wiki/Query_string
    """
    query: str = urlencode(query=params, doseq=True, quote_via=quote)
    return f"{url}?{query}"
