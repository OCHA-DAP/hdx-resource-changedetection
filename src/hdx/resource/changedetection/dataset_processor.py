from typing import List, Tuple, Set, Dict
from urllib.parse import urlsplit

from hatch.cli import self
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.scraper.framework.utilities.reader import Read
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import list_distribute_contents


class DatasetProcessor:
    def __init__(self, configuration: Configuration):
        self._configuration = configuration
        self._netlocs = set()
        self.resources = {}

    def get_all_datasets(self) -> List[Dataset]:
        reader = Read.get_reader()
        return reader.search_datasets(
            filename = "datasets",
            configuration=self._configuration,
            query = self._configuration["query"],
            fq = self._configuration.get("fq"),
            sort="metadata_created asc",
        )

    def process(self, datasets: List[Dataset]) -> None:
        for dataset in datasets:
            for resource in dataset.get_resources():
                url = resource["url"]
                resource_id = resource["id"]
                resource_format = resource["format"]
                size = resource.get("size")
                last_modified = parse_date(resource["last_modified"])
                hash = resource.get("hash")
                self.resources[resource_id] = (url, resource_id, resource_format, size, last_modified, hash)

    def get_resources(self) -> Dict[str, Tuple]:
        return self.resources

    def get_distributed_resources_to_check(self) -> List[Tuple]:
        def get_netloc(x):
            netloc = urlsplit(x[0]).netloc
            self._netlocs.add(netloc)
            return netloc

        return list_distribute_contents(
            list(self.resources.values()), get_netloc
        )

    def get_netlocs(self) -> Set[str]:
        return self._netlocs