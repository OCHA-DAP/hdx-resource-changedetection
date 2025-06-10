import logging
from datetime import datetime
from http import HTTPStatus
from typing import Dict, Tuple

from .utilities import revise_resource, status_lookup
from hdx.utilities.dateparse import parse_date
from hdx.utilities.typehint import ListTuple

logger = logging.getLogger(__name__)


class Results:
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
            log_status = resource_status.get(resource_id)
            resource = self._resources[resource_id]
            dataset_id = resource[3]
            size, last_modified, hash, status = result
            status_str = status_lookup[status]
            log_status["Get Status"] = status_str
            if status != 0 and status != HTTPStatus.OK:
                if status < 0:
                    if status < -10:
                        if status < -100:
                            if not resource[7]:  # currently broken
                                revise_resource(
                                    self._datasets_to_revise,
                                    dataset_id,
                                    resource_id,
                                )
                                log_status["Set Broken"] = "Y"
                        continue
                else:
                    if status != HTTPStatus.TOO_MANY_REQUESTS:
                        if not resource[7]:  # currently broken
                            revise_resource(
                                self._datasets_to_revise, dataset_id, resource_id
                            )
                            log_status["Set Broken"] = "Y"

            resource_info = {}
            update = False
            hash_changed = False
            if status == HTTPStatus.OK:
                etag_str = "ETag"
            else:
                etag_str = "Hash"

            if hash:
                log_status[f"New {etag_str}"] = "Y"
                if hash != resource[6]:
                    resource_info["hash"] = hash
                    hash_changed = True
                    update = True
                    log_status[f"{etag_str} Changed"] = "Y"
                else:
                    log_status[f"{etag_str} Changed"] = "N"

            else:
                log_status[f"New {etag_str}"] = "N"
                if resource[6]:
                    log_status[f"{etag_str} Changed"] = "Y"
                else:
                    log_status[f"{etag_str} Changed"] = "N"

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
                if resource_info:
                    revise_resource(
                        self._datasets_to_revise,
                        dataset_id,
                        resource_id,
                        resource_info,
                    )
                    log_status["Update"] = "Y"

    def get_datasets_to_revise(self) -> Dict[str, Dict]:
        return self._datasets_to_revise
