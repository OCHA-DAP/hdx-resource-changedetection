import logging
from datetime import datetime
from http import HTTPStatus
from typing import Dict, List, Tuple

from .utilities import log_output, revise_resource, status_lookup
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
        self._datasets_to_revise = {}
        self._change_output = {}
        self._broken_output = {}

    def process(self) -> None:
        for resource_id, result in self._results.items():
            what_changed = []
            resource = self._resources[resource_id]
            dataset_id = resource[3]
            size, last_modified, hash, status = result
            if status != 0 and status != HTTPStatus.OK:
                status_str = status_lookup[status]
                if status < 0:
                    if status < -10:
                        dict_of_lists_add(
                            self._change_output, status_str, resource_id
                        )
                        if status < -100:
                            revise_resource(
                                self._datasets_to_revise,
                                dataset_id,
                                resource_id,
                            )
                            dict_of_lists_add(
                                self._broken_output, status_str, resource_id
                            )
                        continue
                else:
                    if status != HTTPStatus.TOO_MANY_REQUESTS:
                        revise_resource(
                            self._datasets_to_revise, dataset_id, resource_id
                        )
                    dict_of_lists_add(
                        self._broken_output, status_str, resource_id
                    )
            else:
                status_str = None

            resource_info = {}
            update = False
            if status == HTTPStatus.OK:
                etag_str = "etag"
            else:
                etag_str = "hash"

            if hash:
                if hash != resource[6]:
                    what_changed.append(etag_str)
                    resource_info["hash"] = hash
                    update = True
            else:
                if resource[6]:
                    status = f"no {etag_str}"
                    what_changed.append(status)

            if size:
                if size != resource[4]:
                    status = "size"
                    what_changed.append(status)
                    resource_info["size"] = size
                    update = True
            else:
                if resource[4]:
                    what_changed.append("no size")

            resource_date = resource[5]
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
                if last_modified and last_modified != resource_date:
                    dt_notz = last_modified.replace(tzinfo=None)
                    resource_info["last_modified"] = dt_notz.isoformat()
                if resource_info:
                    revise_resource(
                        self._datasets_to_revise,
                        dataset_id,
                        resource_id,
                        resource_info,
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

    def get_datasets_to_revise(self) -> Dict[str, Dict]:
        return self._datasets_to_revise
