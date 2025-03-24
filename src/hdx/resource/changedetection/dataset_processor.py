from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlsplit

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.scraper.framework.utilities.reader import Read
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import list_distribute_contents


class DatasetProcessor:
    def __init__(
        self,
        configuration: Configuration,
        netlocs_ignore: Iterable[str] = (),
        task_code: Optional[str] = None,
    ):
        self._configuration = configuration
        self._netlocs = set()
        self._resources = {}
        self._netlocs_ignore = netlocs_ignore
        self._task_code = task_code

    def get_all_datasets(self) -> List[Dataset]:
        reader = Read.get_reader()
        filters = []
        if self._task_code:
            filters.append(f"id:{self._task_code}*")
        filters.append(self._configuration.get("fq"))

        return reader.search_datasets(
            filename="datasets",
            configuration=self._configuration,
            query=self._configuration["query"],
            fq=" ".join(filters),
            sort="metadata_created asc",
        )

    def process(self, datasets: List[Dataset]) -> None:
        for dataset in datasets:
            for resource in dataset.get_resources():
                url = resource["url"]
                netloc = urlsplit(url).netloc
                if netloc in self._netlocs_ignore:
                    continue
                self._netlocs.add(netloc)
                resource_id = resource["id"]
                resource_format = resource["format"]
                dataset_id = dataset["id"]
                size = resource.get("size")
                last_modified = parse_date(resource["last_modified"])
                hash = resource.get("hash")
                self._resources[resource_id] = (
                    url,
                    resource_id,
                    resource_format,
                    dataset_id,
                    size,
                    last_modified,
                    hash,
                )

    def get_resources(self) -> Dict[str, Tuple]:
        return self._resources

    def get_distributed_resources_to_check(self) -> List[Tuple]:
        def get_netloc(x):
            return urlsplit(x[0]).netloc

        return list_distribute_contents(
            list(self._resources.values()), get_netloc
        )

    def get_netlocs(self) -> Set[str]:
        return self._netlocs
