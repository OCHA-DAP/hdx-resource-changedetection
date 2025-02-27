import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


class HeadResults:
    def __init__(
        self, results: Dict[str, Tuple], resources: Dict[str, Tuple]
    ) -> None:
        self.results = results
        self.resources = resources
        self.changes = {}

    def process(self) -> None:
        for resource_id, result in self.results.items():
            what_changed = []
            resource = self.resources[resource_id]
            http_size, http_last_modified, etag, status = result
            if http_size:
                if http_size != resource[3]:
                    what_changed.append("size")
            else:
                what_changed.append("no http size")
            if http_last_modified:
                if http_last_modified != resource[4]:
                    what_changed.append("modified")
            else:
                what_changed.append("no http modified")
            if etag:
                if etag != resource[5]:
                    what_changed.append("etag")
            else:
                what_changed.append("no etag")
            if status != "200":
                what_changed.append(f"status {status}")
            what_changed = "|".join(what_changed)
            self.changes[what_changed] = self.changes.get(what_changed, 0) + 1

    def output(self) -> None:
        for what_changed, count in self.changes.items():
            logger.info(f"{what_changed}: {count}")
