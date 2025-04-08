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
        -3: "SIZE != HTTP SIZE",
        -11: "TOO LARGE TO HASH",
        -101: "UNSPECIFIED SERVER ERROR",
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
        HTTPStatus.REQUEST_TIMEOUT,
        HTTPStatus.CONFLICT,
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_GATEWAY,
        HTTPStatus.SERVICE_UNAVAILABLE,
        HTTPStatus.GATEWAY_TIMEOUT,
    ):
        # These are too common to log by default
        logger.debug(f"Retrying: {ex.status} {ex.message} {ex.request_info.url}")
        return True
    return False


def revise_resource(
    datasets_to_revise: Dict,
    dataset_id: str,
    resource_id: str,
    resource_info: Dict = {"broken_link": True},
) -> None:
    dataset_to_revise = datasets_to_revise.get(dataset_id, {})
    dataset_to_revise["match"] = {"id": dataset_id}
    dataset_to_revise[f"update__resources__{resource_id}"] = resource_info
    datasets_to_revise[dataset_id] = dataset_to_revise
