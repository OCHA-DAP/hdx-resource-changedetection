import logging

import aiohttp

logger = logging.getLogger(__name__)

status_lookup = {
    -99: "Unspecified Server Error",
    -100: "Resource too large",
    -1: "Mimetype != HDX Format",
    -2: "Signature != HDX Format",
}


def is_server_error(ex: BaseException) -> bool:
    if isinstance(ex, aiohttp.ServerTimeoutError):
        logger.info(f"Retrying {ex.strerror} {ex.filename}")
        return True
    if isinstance(ex, aiohttp.ClientResponseError) and ex.status in (
        408,
        409,
        429,
        500,
        502,
        503,
        504,
    ):
        #        logger.info(f"Retrying {ex.status} {ex.message} {ex.request_info.url}")
        return True
    return False
