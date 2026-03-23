"""Command line interface for change detection"""

import logging
from os.path import expanduser, join
from urllib.parse import urlsplit

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.scraper.framework.utilities.reader import Read
from hdx.utilities.dateparse import now_utc
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)

from . import __version__
from .config import LOOKUP, UPDATED_BY_SCRIPT, init_logging
from .dataset_processor import DatasetProcessor
from .results import Results
from .retrieval import Retrieval
from hdx.resource.changedetection.dataset_updater import DatasetUpdater
from hdx.resource.changedetection.task_manager import TaskManager
from hdx.resource.changedetection.utilities import get_status_count, output_status_count

init_logging()
logger = logging.getLogger(__name__)


def main(
    save: bool = False,
    use_saved: bool = False,
    csv_path: str = "",
    revise: bool = False,
    use_redis: bool = False,
    specific_task_code: str = None,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save: Save downloaded data. Defaults to False.
        use_saved: Use saved data. Defaults to False.
        csv_path: Path to CSV file. Defaults to "" (don't generate)
        revise: Whether to revise datasets. Defaults to False.
        use_redis: Whether to use redis and split job into tasks. Defaults to False.
        specific_task_code: Specific task code to run. Defaults to None (run all tasks).
    Returns:
        None
    """
    logger.info(f"##### {LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    if not User.check_current_user_organization_access("hdx", "create_dataset"):
        raise PermissionError("API Token does not give access to HDX organisation!")
    task_manager = TaskManager()
    temp_folder = f"{LOOKUP}_{task_manager.instance_id}"
    with wheretostart_tempdir_batch(temp_folder) as info:
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

        total_results = Results(today, {}, {})
        total_resource_status = {}
        task_code = None
        while not use_redis or (task_code := task_manager.sync_acquire_task()):
            netlocs_ignore = {
                "data.humdata.org",
                urlsplit(configuration.get_hdx_site_url()).netloc,
            }
            formats_ignore = {"web app"}
            dataset_processor = DatasetProcessor(
                configuration, netlocs_ignore, formats_ignore, task_code
            )
            datasets = dataset_processor.get_all_datasets()
            dataset_processor.process(datasets)

            resources_to_check = dataset_processor.get_distributed_resources_to_check()
            netlocs = dataset_processor.get_netlocs()
            retrieval = Retrieval(configuration.get_user_agent(), netlocs)
            results = retrieval.retrieve(resources_to_check)

            resources = dataset_processor.get_resources()
            total_results.add_more_results(results, resources)
            resource_status = {}
            results = Results(today, results, resources)
            results.process(resource_status)

            datasets_to_revise = results.get_datasets_to_revise()

            dataset_updater = DatasetUpdater(configuration, datasets_to_revise)
            dataset_updater.process(revise)

            total_resource_status.update(resource_status)
            status_count = get_status_count(resource_status)

            if use_redis:
                task_manager.sync_finish_task(task_code)
            else:
                output_status_count(status_count, csv_path)
                break

            # if task_code == "2":

            #     break

        if use_redis:
            logger.info("Finished all tasks")
            total_results.process(total_resource_status)
            status_count = get_status_count(total_resource_status)
            output_status_count(status_count, csv_path)

    logger.info(f"{UPDATED_BY_SCRIPT} completed!")


def run_cli() -> None:
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
