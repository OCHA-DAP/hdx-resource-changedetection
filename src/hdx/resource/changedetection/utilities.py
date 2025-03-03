import aiohttp

status_lookup = {
    -99: "Unspecified Server Error",
    -100: "Resource too large",
    -1: "Mimetype != HDX Format",
    -2: "Signature != HDX Format",
}


def is_server_error(exception):
    return isinstance(exception, aiohttp.ServerTimeoutError) or (
        isinstance(exception, aiohttp.ClientResponseError)
        and exception.status in (408, 409, 429, 500, 502, 503, 504)
    )
