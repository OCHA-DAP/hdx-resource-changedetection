import logging
from http import HTTPStatus
from typing import Dict, Tuple

from .utilities import log_output, status_lookup
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add

logger = logging.getLogger(__name__)


class Results:
    def __init__(
        self, results: Dict[str, Tuple], resources: Dict[str, Tuple]
    ) -> None:
        self.results = results
        self.resources = resources
        self._resources_to_update = {}
        self._changes = {}

    def process(self) -> None:
        for resource_id, result in self.results.items():
            what_changed = []
            resource = self.resources[resource_id]
            http_size, http_last_modified, etag, status = result
            if status != 0 and status != HTTPStatus.OK:
                status_str = status_lookup[status]
                if status < 0:
                    if status < -90:
                        dict_of_lists_add(
                            self._changes, what_changed, resource_id
                        )
                        continue
            else:
                status_str = None
            update = False
            if etag:
                if etag != resource[5]:
                    what_changed.append("etag")
                    update = True
            else:
                status = "no etag"
                what_changed.append(status)
            if http_size:
                if http_size != resource[3]:
                    status = "size"
                    what_changed.append(status)
                    update = True
            else:
                what_changed.append("no size")
            if http_last_modified:
                http_last_modified = parse_date(http_last_modified)
                if http_last_modified != resource[4]:
                    status = "modified"
                    what_changed.append(status)
                    update = True
            else:
                what_changed.append("no modified")
            if update:
                self._resources_to_update[resource_id] = (
                    http_size,
                    http_last_modified,
                    etag,
                )
            what_changed = "|".join(what_changed)
            if status_str:
                what_changed = f"{status_str}  {what_changed}"
            dict_of_lists_add(self._changes, what_changed, resource_id)

    def output(self) -> None:
        logger.info("\nChanges detected:")
        log_output(self._changes)

    def get_resources_to_update(self) -> Dict[str, Tuple]:
        return self._resources_to_update
