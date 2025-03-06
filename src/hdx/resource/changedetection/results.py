import logging
from datetime import datetime
from http import HTTPStatus
from typing import Dict, List, Tuple

from .utilities import log_output, status_lookup
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
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
        self._resources_to_update = {}
        self._broken_resources = {}
        self._change_output = {}
        self._broken_output = {}

    def process(self) -> None:
        for resource_id, result in self._results.items():
            what_changed = []
            resource = self._resources[resource_id]
            size, last_modified, etag, status = result
            if status != 0 and status != HTTPStatus.OK:
                status_str = status_lookup[status]
                if status < 0:
                    if status < -10:
                        dict_of_lists_add(
                            self._change_output, status_str, resource_id
                        )
                        if status < -100:
                            self._broken_resources[resource_id] = resource
                            dict_of_lists_add(
                                self._broken_output, status_str, resource_id
                            )
                        continue
                else:
                    if status != HTTPStatus.TOO_MANY_REQUESTS:
                        self._broken_resources[resource_id] = resource
                    dict_of_lists_add(
                        self._broken_output, status_str, resource_id
                    )
            else:
                status_str = None

            update = False
            if status == HTTPStatus.OK:
                etag_str = "etag"
            else:
                etag_str = "hash"

            if etag:
                if etag != resource[5]:
                    what_changed.append(etag_str)
                    update = True
            else:
                if resource[5]:
                    status = f"no {etag_str}"
                    what_changed.append(status)

            if size:
                if size != resource[3]:
                    status = "size"
                    what_changed.append(status)
                    update = True
            else:
                if resource[3]:
                    what_changed.append("no size")

            resource_date = resource[4]
            if last_modified:
                last_modified = parse_date(last_modified)
                if not resource_date or last_modified > resource_date:
                    status = "modified"
                    what_changed.append(status)
                    update = True
                elif last_modified < resource_date:
                    what_changed.append("modified http<resource")
            else:
                if resource_date:
                    what_changed.append("no modified")

            if update:
                if not last_modified or (
                    resource_date and last_modified <= resource_date
                ):
                    if resource_date:
                        if self._today > resource_date:
                            last_modified = self._today
                            what_changed.append("today")
                        else:
                            last_modified = resource_date
                    else:
                        last_modified = self._today
                        what_changed.append("today")
                self._resources_to_update[resource_id] = (
                    size,
                    last_modified,
                    etag,
                )

            what_changed = "|".join(what_changed)
            if not what_changed:
                what_changed = "nothing"
            if status_str:
                what_changed = f"{status_str}  {what_changed}"
            dict_of_lists_add(self._change_output, what_changed, resource_id)

    def output(self) -> Tuple[List[str], List[str]]:
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
        return change_output, broken_output

    def get_resources_to_update(self) -> Dict[str, Tuple]:
        return self._resources_to_update

    def get_broken_resources(self) -> Dict[str, Tuple]:
        return self._broken_resources
