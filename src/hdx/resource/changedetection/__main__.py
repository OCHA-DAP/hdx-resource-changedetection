"""Entry point to start change detection"""

import logging
from os.path import expanduser, join
from urllib.parse import urlsplit

from . import __version__
from .dataset_processor import DatasetProcessor
from .head_results import HeadResults
from .head_retrieval import HeadRetrieval
from .results import Results
from .retrieval import Retrieval
from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.resource.changedetection.dataset_updater import DatasetUpdater
from hdx.scraper.framework.utilities.reader import Read
from hdx.utilities.dateparse import now_utc
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)

setup_logging()
logger = logging.getLogger(__name__)

lookup = "hdx-resource-changedetection"
updated_by_script = "HDX Resource Change Detection"


def main(
    save: bool = False,
    use_saved: bool = False,
    revise: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.
        revise (bool): Whether to revise datasets. Defaults to False.
    Returns:
        None
    """
    logger.info(f"##### {lookup} version {__version__} ####")
    configuration = Configuration.read()
    if not User.check_current_user_organization_access(
        "hdx", "create_dataset"
    ):
        raise PermissionError(
            "API Token does not give access to HDX organisation!"
        )
    with wheretostart_tempdir_batch(lookup) as info:
        folder = info["folder"]

        today = now_utc()
        Read.create_readers(
            folder,
            "saved_data",
            folder,
            save,
            use_saved,
            hdx_auth=configuration.get_api_key(),
            today=today,
        )
        netlocs_ignore = {
            "data.humdata.org",
            urlsplit(configuration.get_hdx_site_url()).netloc,
        }
        dataset_processor = DatasetProcessor(configuration, netlocs_ignore)
        datasets = dataset_processor.get_all_datasets()
        dataset_processor.process(datasets)

        resources_to_check = (
            dataset_processor.get_distributed_resources_to_check()
        )
        netlocs = dataset_processor.get_netlocs()
        retrieval = HeadRetrieval(configuration.get_user_agent(), netlocs)
        results = retrieval.retrieve(resources_to_check)

        head_results = HeadResults(results, dataset_processor.get_resources())
        head_results.process()
        head_results.output()

        resources_to_get = head_results.get_distributed_resources_to_get()
        netlocs = head_results.get_netlocs()
        retrieval = Retrieval(configuration.get_user_agent(), netlocs)
        results = retrieval.retrieve(resources_to_get)

        results = Results(today, results, dataset_processor.get_resources())
        results.process()
        results.output()

        datasets_to_revise = head_results.get_datasets_to_revise()
        datasets_to_revise.update(results.get_datasets_to_revise())

        dataset_updater = DatasetUpdater(configuration, datasets_to_revise)
        dataset_updater.process(revise)
        dataset_updater.output()

    logger.info(f"{updated_by_script} completed!")


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
