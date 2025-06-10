import logging
from http import HTTPStatus
from typing import Dict, List, Set, Tuple
from urllib.parse import urlsplit

from .utilities import get_blank_log_status, revise_resource, status_lookup
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import (
    list_distribute_contents,
)
from hdx.utilities.typehint import ListTuple

logger = logging.getLogger(__name__)


class HeadResults:
    def __init__(
        self, results: Dict[str, ListTuple], resources: Dict[str, Tuple]
    ) -> None:
        self._results = results
        self._resources = resources
        self._resources_to_get = {}
        self._datasets_to_revise = {}
        self._netlocs = set()

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
            size, last_modified, etag, status = result
            status_str = status_lookup[status]
            log_status["Head Status"] = status_str
            if status != HTTPStatus.OK:
                if status in (
                    HTTPStatus.FORBIDDEN,
                    HTTPStatus.METHOD_NOT_ALLOWED,
                    HTTPStatus.REQUEST_TIMEOUT,
                    HTTPStatus.CONFLICT,
                    HTTPStatus.TOO_MANY_REQUESTS,
                ):
                    # Server may not like HEAD requests or too many requests
                    self._resources_to_get[resource_id] = resource
                else:
                    if not existing_broken:  # currently broken
                        revise_resource(
                            self._datasets_to_revise, dataset_id, resource_id
                        )
                        log_status["Set Broken"] = "Y"
                resource_status[resource_id] = log_status
                continue

            get_resource = False

            resource_info = {}
            if etag:
                log_status["New ETag"] = "Y"
                if etag != existing_hash:
                    resource_info["hash"] = etag
                    log_status["ETag Changed"] = "Y"
                else:
                    log_status["ETag Changed"] = "N"
            else:
                log_status["New ETag"] = "N"
                get_resource = True
                if existing_hash:
                    log_status["ETag Changed"] = "Y"
                else:
                    log_status["ETag Changed"] = "N"

            if size:
                log_status["New Size"] = "Y"
                if size != existing_size:
                    log_status["Size Changed"] = "Y"
                    if resource_info:
                        resource_info["size"] = size
                    else:
                        get_resource = True
                else:
                    log_status["Size Changed"] = "N"
            else:
                log_status["New Size"] = "N"
                if existing_size:
                    log_status["Size Changed"] = "Y"
                else:
                    log_status["Size Changed"] = "N"

            if last_modified:
                log_status["New Modified"] = "Y"
                last_modified = parse_date(last_modified)
                if not resource_date or last_modified > resource_date:
                    log_status["Modified Changed"] = "Y"
                    log_status["Modified Newer"] = "Y"
                    if resource_info:
                        dt_notz = last_modified.replace(tzinfo=None)
                        resource_info["last_modified"] = dt_notz.isoformat()
                        log_status["Modified Value"] = "http"
                    else:
                        get_resource = True
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

            if get_resource:
                self._resources_to_get[resource_id] = resource
            if resource_info:
                revise_resource(
                    self._datasets_to_revise,
                    dataset_id,
                    resource_id,
                    resource_info,
                )
                log_status["Update"] = "Y"
            resource_status[resource_id] = log_status

    def get_distributed_resources_to_get(self) -> List[Tuple]:
        def get_netloc(x):
            netloc = urlsplit(x[0]).netloc
            self._netlocs.add(netloc)
            return netloc

        return list_distribute_contents(
            list(self._resources_to_get.values()), get_netloc
        )

    def get_netlocs(self) -> Set[str]:
        return self._netlocs

    def get_datasets_to_revise(self) -> Dict[str, Dict]:
        return self._datasets_to_revise
