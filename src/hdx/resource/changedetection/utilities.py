import logging
from http import HTTPStatus
from typing import Dict, List

import aiohttp

logger = logging.getLogger(__name__)

status_lookup = {i.value: i.name for i in HTTPStatus}
status_lookup.update(
    {
        -1: "MIMETYPE != HDX FORMAT",
        -2: "SIGNATURE != HDX FORMAT",
        -99: "TOO LARGE TO HASH",
        -100: "UNSPECIFIED SERVER ERROR",
    }
)


def log_output(status_to_resourceids: Dict) -> List[str]:
    output = []
    for status in sorted(status_to_resourceids):
        resource_ids = status_to_resourceids[status]
        count = len(resource_ids)
        if count < 5:
            resource_ids = ", ".join(resource_ids)
            output_str = f"{status}: {resource_ids}"
        else:
            output_str = f"{status}: {count}"
        logger.info(output_str)
        output.append(output_str)
    return output


def is_server_error(ex: BaseException) -> bool:
    if isinstance(ex, aiohttp.ServerTimeoutError):
        logger.info(f"Retrying: {str(ex)}")
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
        # These are too common to log by default
        logger.debug(
            f"Retrying: {ex.status} {ex.message} {ex.request_info.url}"
        )
        return True
    return False
