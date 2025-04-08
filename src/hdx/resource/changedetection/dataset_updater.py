import logging
from typing import Any, Dict

from tqdm import tqdm

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

logger = logging.getLogger(__name__)


class DatasetUpdater:
    def __init__(
        self, configuration: Configuration, datasets_to_revise: Dict[str, Any]
    ):
        self.configuration = configuration
        self._datasets_to_revise = datasets_to_revise

    def process(self, revise: bool) -> None:
        if not revise:
            return
        logger.info("\n")
        logger.info(f"Will revise {len(self._datasets_to_revise)} datasets")
        for dataset_to_revise in tqdm(self._datasets_to_revise.values()):
            dataset = Dataset(dataset_to_revise, configuration=self.configuration)
            dataset_to_revise["update__batch_mode"] = "KEEP_OLD"
            dataset_to_revise["update__skip_validation"] = "true"
            dataset_to_revise["filter"] = "-extras"
            dataset._write_to_hdx(
                "revise",
                dataset_to_revise,
                id_field_name="match",
            )
