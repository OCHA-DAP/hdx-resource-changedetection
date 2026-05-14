import itertools
import logging
from collections import defaultdict
from collections.abc import Callable, Sequence
from http import HTTPStatus
from typing import Any

import aiohttp
from hdx.utilities.saver import save_iterable
from prettytable import PrettyTable

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


def get_blank_log_status() -> dict[str, str]:
    return {
        "Existing Hash": "",
        "Existing Modified": "",
        "Existing Size": "",
        "Existing Broken": "",
        "HTTP Status": "",
        "Sig Match": "",
        "Mime Match": "",
        "Size Match": "",
        "Set Broken": "N",
        "Has ETag": "",
        "Has Modified": "",
        "Modified Changed": "",
        "Modified Newer": "",
        "Modified Value": "",
        "Has Size": "",
        "Size Changed": "",
        "Has Hash": "",
        "Hash Changed": "",
        "Hash Type": "",
        "Update": "N",
        "Error": "",
    }


def get_status_count(resource_status: dict[str, str]) -> dict[str, int]:
    status_count = {}
    for resource_id, status in resource_status.items():
        key = tuple(status.values())
        status_count[key] = status_count.get(key, 0) + 1
    return status_count


def output_status_count(status_count: dict[str, int], path: str) -> None:
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
        save_iterable(path, rows)


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
    datasets_to_revise: dict,
    dataset_id: str,
    resource_id: str,
    resource_info: dict,
) -> None:
    dataset_to_revise = datasets_to_revise.get(dataset_id, {})
    dataset_to_revise["match"] = {"id": dataset_id}
    if "broken_link" not in resource_info:
        resource_info["broken_link"] = False
    dataset_to_revise[f"update__resources__{resource_id}"] = resource_info
    datasets_to_revise[dataset_id] = dataset_to_revise


def list_distribute_contents(
    input_list: Sequence,
    function: Callable[[Any], Any] = lambda x: x,
    chunk_size: int = 15,
) -> list:
    """
    Distribute the contents of a list by interleaving chunks.
    This prevents 'Keep-Alive' connection thrashing in aiohttp.
    """
    if not input_list:
        return []

    # 1. Group everything by host
    dictionary = defaultdict(list)
    for obj in input_list:
        dictionary[function(obj)].append(obj)

    # 2. Break each host's massive list into smaller chunks
    chunked_piles = []
    for key in sorted(dictionary.keys()):
        pile = dictionary[key]
        # Slice the pile into chunks of `chunk_size`
        chunks = [pile[i : i + chunk_size] for i in range(0, len(pile), chunk_size)]
        chunked_piles.append(chunks)

    # 3. Interleave the chunks
    # zip_longest takes one chunk from Host A, one from Host B, etc., then loops.
    result = []
    for interleaved_chunks in itertools.zip_longest(*chunked_piles):
        for chunk in interleaved_chunks:
            if chunk is not None:
                result.extend(chunk)

    return result
