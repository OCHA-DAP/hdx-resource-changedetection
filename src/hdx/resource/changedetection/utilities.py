import logging
from http import HTTPStatus
from typing import Dict

import aiohttp
from prettytable import PrettyTable

from hdx.utilities.dictandlist import write_list_to_csv

logger = logging.getLogger(__name__)

status_lookup = {i.value: i.name for i in HTTPStatus}
status_lookup.update(
    {
        0: "OK",
        -1: "MIMETYPE != HDX FORMAT",
        -2: "SIGNATURE != HDX FORMAT",
        -3: "SIZE != HTTP SIZE",
        -11: "TOO LARGE TO HASH",
        -101: "UNSPECIFIED SERVER ERROR",
    }
)


def get_blank_log_status() -> Dict[str, str]:
    return {
        "Existing Hash": "",
        "Existing Modified": "",
        "Existing Size": "",
        "Existing Broken": "",
        "Set Broken": "N",
        "Head Status": "",
        "Head Error": "",
        "Get Status": "",
        "Get Error": "",
        "New ETag": "",
        "ETag Changed": "",
        "New Modified": "",
        "Modified Changed": "",
        "Modified Newer": "",
        "Modified Value": "",
        "New Size": "",
        "Size Changed": "",
        "New Hash": "",
        "Hash Changed": "",
        "Update": "N",
    }


def get_status_count(resource_status: Dict[str, str]) -> Dict[str, int]:
    status_count = {}
    for resource_id, status in resource_status.items():
        key = tuple(status.values())
        status_count[key] = status_count.get(key, 0) + 1
    return status_count


def output_status_count(status_count: Dict[str, int], path: str) -> None:
    log_status = get_blank_log_status()
    table = PrettyTable()
    headers = list(log_status.keys()) + ["Number"]
    rows = [headers]
    table.field_names = headers
    for key in sorted(status_count):
        row = list(key) + [status_count[key]]
        table.add_row(row)
        rows.append(row)
    print(table)
    if path:
        write_list_to_csv(path, rows)


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
