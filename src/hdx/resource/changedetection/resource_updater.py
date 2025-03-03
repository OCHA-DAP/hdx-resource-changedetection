import logging
from datetime import datetime

from hdx.api.configuration import Configuration

logger = logging.getLogger(__name__)


class ResourceUpdater:
    def __init__(self, configuration: Configuration):
        self.configuration = configuration
        self._count = 0

    def update_resource(
        self, resource_id: str, size: int, last_modified: datetime, hash: str
    ) -> None:
        self._count += 1
        # resource = Resource.read_from_hdx(resource_id, configuration=self.configuration)
        # if size:
        # resource["size"] = size
        # if last_modified:
        #     resource["last_modified"] = last_modified.isoformat()
        # if hash:
        #     resource["hash"] = hash
        # resource.update_in_hdx(
        #     operation="patch",
        #     batch_mode="KEEP_OLD",
        #     skip_validation=True,
        #     ignore_check=True,
        # )

    def output(self) -> None:
        logger.info("\n")
        logger.info(f"Patching {self._count} resources")
