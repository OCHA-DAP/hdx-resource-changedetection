import aiohttp


def is_server_error(exception):
    return isinstance(exception, aiohttp.ServerTimeoutError) or (
        isinstance(exception, aiohttp.ClientResponseError)
        and exception.status in (408, 409, 429, 500, 502, 503, 504)
    )
