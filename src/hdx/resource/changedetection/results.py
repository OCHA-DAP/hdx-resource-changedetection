import logging
from datetime import datetime
from http import HTTPStatus
from typing import Dict, Tuple

from .utilities import get_blank_log_status, revise_resource, status_lookup
from hdx.utilities.dateparse import parse_date
)
from hdx.utilities.typehint import ListTuple

logger = logging.getLogger(__name__)


class HeadResults:
    def __init__(
        self,
        today: datetime,
        results: Dict[str, ListTuple],
        resources: Dict[str, Tuple],
    ) -> None:
        self._today = today
        self._results = results
        self._resources = resources
        self._datasets_to_revise = {}

    def add_more_results(
        self, results: Dict[str, ListTuple], resources: Dict[str, Tuple]
    ):
        self._results.update(results)
        self._resources.update(resources)

    def process(self, resource_status: Dict[str, Dict]) -> None:
        for resource_id, result in self._results.items():
            log_status = get_blank_log_status()
            resource = self._resources[resource_id]
            existing_hash = resource[6]
            if existing_hash:
                log_status["Existing Hash"] = "Y"
            else:
                log_status["Existing Hash"] = "N"
            existing_size = resource[4]
            if existing_size:
                log_status["Existing Size"] = "Y"
            else:
                log_status["Existing Size"] = "N"
            resource_date = resource[5]
            if resource_date:
                log_status["Existing Modified"] = "Y"
            else:
                log_status["Existing Modified"] = "N"
            existing_broken = resource[7]
            if existing_broken:
                log_status["Existing Broken"] = "Y"
            else:
                log_status["Existing Broken"] = "N"
            dataset_id = resource[3]
            (
                size,
                last_modified,
                etag,
                final_hash,
                sig_match,
                mime_match,
                http_status,
                status,
            ) = result
            log_status["HTTP Status"] = status_lookup[http_status]
            if http_status != HTTPStatus.OK:
                if http_status not in (
                    HTTPStatus.FORBIDDEN,
                    HTTPStatus.METHOD_NOT_ALLOWED,
                    HTTPStatus.REQUEST_TIMEOUT,
                    HTTPStatus.CONFLICT,
                    HTTPStatus.TOO_MANY_REQUESTS,
                ):
                    if not existing_broken:  # currently broken
                        revise_resource(
                            self._datasets_to_revise, dataset_id, resource_id
                        )
                        log_status["Set Broken"] = "Y"
                resource_status[resource_id] = log_status
                continue

            resource_info = {}
            update = False
            hash_changed = False

            log_status["Sig Match"] = "Y" if sig_match else "N"
            log_status["Mime Match"] = "Y" if mime_match else "N"
            if etag:
                log_status["New ETag"] = "Y"
                if etag != existing_hash:
                    log_status["ETag Changed"] = "Y"
                else:
                    log_status["ETag Changed"] = "N"
            else:
                log_status["New ETag"] = "N"
                log_status["ETag Changed"] = ""

            if final_hash:
                match status:
                    case 1:
                        log_status["Hash Type"] = "md5"
                    case 2:
                        log_status["Hash Type"] = "md5-xl"
                    case 3:
                        log_status["Hash Type"] = "crc"
                    case 4:
                        log_status["Hash Type"] = "md5-fb"
                    case 5:
                        log_status["Hash Type"] = "etag"
                    case 6:
                        log_status["Hash Type"] = "etag-sz"
                    case 7:
                        log_status["Hash Type"] = "crc-as"
                    case _:
                        log_status["Hash Type"] = ""
                log_status[f"New Hash"] = "Y"
                if final_hash != resource[6]:
                    if log_status["Hash Type"]:
                        resource_info["hash"] = final_hash
                        hash_changed = True
                        update = True
                    log_status[f"Hash Changed"] = "Y"
                else:
                    log_status[f"Hash Changed"] = "N"
            else:
                log_status[f"New Hash"] = "N"
                if resource[6]:
                    log_status[f"Hash Changed"] = "Y"
                else:
                    log_status[f"Hash Changed"] = "N"
            if size:
                log_status["New Size"] = "Y"
                if size != resource[4]:
                    log_status["Size Changed"] = "Y"
                    resource_info["size"] = size
                    update = True
                else:
                    log_status["Size Changed"] = "N"
            else:
                log_status["New Size"] = "N"
                if resource[4]:
                    log_status["Size Changed"] = "Y"
                else:
                    log_status["Size Changed"] = "N"

            resource_date = resource[5]
            if last_modified:
                log_status["New Modified"] = "Y"
                last_modified = parse_date(last_modified)
                if not resource_date or last_modified > resource_date:
                    log_status["Modified Changed"] = "Y"
                    log_status["Modified Newer"] = "Y"
                    # Only update if hash has also changed
                    if hash_changed:
                        update = True
                elif last_modified < resource_date:
                    log_status["Modified Changed"] = "Y"
                    log_status["Modified Newer"] = "N"
                else:
                    log_status["Modified Changed"] = "N"
            else:
                log_status["New Modified"] = "N"
                if resource_date:
                    log_status["Modified Changed"] = "Y"
                else:
                    log_status["Modified Changed"] = "N"

            if update:
                if not last_modified or (
                    resource_date and last_modified <= resource_date
                ):
                    if resource_date:
                        if self._today > resource_date:
                            last_modified = self._today
                            log_status["Modified Value"] = "today"
                        else:
                            last_modified = resource_date
                    else:
                        last_modified = self._today
                        log_status["Modified Value"] = "today"
                else:
                    log_status["Modified Value"] = "http"
                # Only update last modified if hash has changed
                if hash_changed and last_modified and last_modified != resource_date:
                    dt_notz = last_modified.replace(tzinfo=None)
                    resource_info["last_modified"] = dt_notz.isoformat()

                match status:
                    case -1 | -2 | -3 | -4:
                        log_status["Warning"] = "HTTP Size!=Size"
                    case -6:
                        log_status["Error"] = "Too big, no etag"
                    case -10:
                        log_status["Error"] = "ClientResponseError"
                    case -11:
                        log_status["Error"] = "Unknown Error"
                if resource_info:
                    revise_resource(
                        self._datasets_to_revise,
                        dataset_id,
                        resource_id,
                        resource_info,
                    )
                    log_status["Update"] = "Y"
            resource_status[resource_id] = log_status

    def get_datasets_to_revise(self) -> Dict[str, Dict]:
        return self._datasets_to_revise
