import logging
from typing import Dict, Tuple

from hdx.resource.changedetection.utilities import status_lookup
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
            if status != 0 and status != 200:
                status_str = status_lookup.get(status, f"status {status}")
                if status < 0:
                    if status < -90:
                        dict_of_lists_add(
                            self._changes, what_changed, resource_id
                        )
                        continue
                what_changed.append(status_str)
            if etag:
                if etag != resource[5]:
                    what_changed.append("etag")
            else:
                status = "no etag"
                what_changed.append(status)
            if http_size:
                if http_size != resource[3]:
                    status = "size"
                    what_changed.append(status)
            else:
                what_changed.append("no size")
            if http_last_modified:
                http_last_modified = parse_date(http_last_modified)
                if http_last_modified != resource[4]:
                    status = "modified"
                    what_changed.append(status)
            else:
                what_changed.append("no modified")
            what_changed = "|".join(what_changed)
            dict_of_lists_add(self._changes, what_changed, resource_id)
            if what_changed:
                self._resources_to_update[resource_id] = (
                    http_size,
                    http_last_modified,
                    etag,
                )

    def output(self) -> None:
        logger.info("\nChanges detected:")
        for what_changed, resource_ids in self._changes.items():
            count = len(resource_ids)
            if count < 5:
                resource_ids = ", ".join(resource_ids)
                logger.info(f"{what_changed}: {resource_ids}")
            else:
                logger.info(f"{what_changed}: {count}")

    def get_resources_to_update(self) -> Dict[str, Tuple]:
        return self._resources_to_update
