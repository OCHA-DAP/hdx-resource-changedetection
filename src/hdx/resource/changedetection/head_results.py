import logging
from typing import Dict, List, Set, Tuple
from urllib.parse import urlsplit

from hdx.utilities.dictandlist import list_distribute_contents

logger = logging.getLogger(__name__)


class HeadResults:
    def __init__(
        self, results: Dict[str, Tuple], resources: Dict[str, Tuple]
    ) -> None:
        self.results = results
        self.resources = resources
        self.changes = {}
        self._resources_to_get = {}
        self._netlocs = set()

    def process(self) -> None:
        for resource_id, result in self.results.items():
            what_changed = []
            resource = self.resources[resource_id]
            http_size, http_last_modified, etag, status = result
            if status != 200:
                if status == 403:
                    # Server doesn't like HEAD requests
                    self._resources_to_get[resource_id] = resource
                what_changed = f"status {status}"
                self.changes[what_changed] = (
                    self.changes.get(what_changed, 0) + 1
                )
                continue
            get_resource = False
            etag_unchanged = True
            if etag:
                if etag != resource[5]:
                    what_changed.append("etag")
                    etag_unchanged = False
            else:
                what_changed.append("no etag")
                get_resource = True
            if http_size:
                if http_size != resource[3]:
                    what_changed.append("size")
                    if etag_unchanged:
                        get_resource = True
            else:
                what_changed.append("no http size")
            if http_last_modified:
                if http_last_modified != resource[4]:
                    what_changed.append("modified")
                    if etag_unchanged:
                        get_resource = True
            else:
                what_changed.append("no http modified")
            what_changed = "|".join(what_changed)
            self.changes[what_changed] = self.changes.get(what_changed, 0) + 1
            if get_resource:
                self._resources_to_get[resource_id] = resource

    def output(self) -> None:
        for what_changed, count in self.changes.items():
            logger.info(f"{what_changed}: {count}")

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
