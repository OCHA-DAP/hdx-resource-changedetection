import logging
from http import HTTPStatus
from typing import Dict, List, Set, Tuple
from urllib.parse import urlsplit

from .utilities import log_output, revise_resource, status_lookup
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import (
    dict_of_lists_add,
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
        self._change_output = {}
        self._netlocs = set()
        self._get_output = {}
        self._broken_output = {}

    def add_more_results(
        self, results: Dict[str, ListTuple], resources: Dict[str, Tuple]
    ):
        self._results.update(results)
        self._resources.update(resources)

    def process(self) -> None:
        for resource_id, result in self._results.items():
            what_changed = []
            why_get = []
            resource = self._resources[resource_id]
            dataset_id = resource[3]
            size, last_modified, etag, status = result
            if status != HTTPStatus.OK:
                status_str = status_lookup[status]
                if status in (
                    HTTPStatus.FORBIDDEN,
                    HTTPStatus.METHOD_NOT_ALLOWED,
                    HTTPStatus.REQUEST_TIMEOUT,
                    HTTPStatus.CONFLICT,
                    HTTPStatus.TOO_MANY_REQUESTS,
                ):
                    # Server may not like HEAD requests or too many requests
                    self._resources_to_get[resource_id] = resource
                    dict_of_lists_add(self._get_output, status_str, resource_id)
                else:
                    revise_resource(self._datasets_to_revise, dataset_id, resource_id)
                    dict_of_lists_add(self._broken_output, status_str, resource_id)
                dict_of_lists_add(self._change_output, status_str, resource_id)
                continue

            get_resource = False

            resource_info = {}
            if etag:
                if etag != resource[6]:
                    what_changed.append("etag")
                    resource_info["hash"] = etag
            else:
                status = "no etag"
                why_get.append(status)
                get_resource = True
                if resource[6]:
                    what_changed.append(status)

            if size:
                if size != resource[4]:
                    status = "size"
                    what_changed.append(status)
                    if resource_info:
                        resource_info["size"] = size
                    else:
                        why_get.append(status)
                        get_resource = True
            else:
                if resource[4]:
                    what_changed.append("no size")

            if last_modified:
                last_modified = parse_date(last_modified)
                resource_date = resource[5]
                if not resource_date or last_modified > resource_date:
                    status = "modified"
                    what_changed.append(status)
                    if resource_info:
                        dt_notz = last_modified.replace(tzinfo=None)
                        resource_info["last_modified"] = dt_notz.isoformat()
                    else:
                        why_get.append(status)
                        get_resource = True
                elif last_modified < resource_date:
                    what_changed.append("modified http<resource")
            else:
                if resource[5]:
                    what_changed.append("no modified")

            what_changed = "|".join(what_changed)
            if not what_changed:
                what_changed = "nothing"
            dict_of_lists_add(self._change_output, what_changed, resource_id)
            if get_resource:
                self._resources_to_get[resource_id] = resource
                why_get = "|".join(why_get)
                dict_of_lists_add(self._get_output, why_get, resource_id)
            if resource_info:
                revise_resource(
                    self._datasets_to_revise,
                    dataset_id,
                    resource_id,
                    resource_info,
                )

    def output(self) -> Tuple[List[str], List[str], List[str]]:
        if self._change_output:
            logger.info("\nChanges detected:")
            change_output = log_output(self._change_output)
        else:
            change_output = []
        if self._broken_output:
            logger.info("\nThese are broken:")
            broken_output = log_output(self._broken_output)
        else:
            broken_output = []
        if self._get_output:
            logger.info("\nWill get these:")
            get_output = log_output(self._get_output)
        else:
            get_output = []
        return change_output, broken_output, get_output

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
