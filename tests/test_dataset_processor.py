from datetime import datetime, timezone
from os.path import join
from urllib.parse import urlsplit

from hdx.resource.changedetection.dataset_processor import DatasetProcessor
from hdx.scraper.framework.utilities.reader import Read
from hdx.utilities.dateparse import parse_date
from hdx.utilities.path import temp_dir


class TestDatasetProcessor:
    def test_dataset_processor(self, configuration, folder):
        with temp_dir(
            "TestDatasetProcessor",
            delete_on_success=True,
            delete_on_failure=False,
        ) as temp_folder:
            today = parse_date("2023-10-11")
            Read.create_readers(
                temp_folder,
                join(folder, "input"),
                temp_folder,
                False,
                True,
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
            assert resources_to_check == [
                (
                    "https://drive.google.com/uc?export=download&id=1yK5olSSLBKmfE0T_EgTP_d8l-lTFfz1I",
                    "959e78d6-fece-4a35-a0ec-d4aef3405d4f",
                    "csv",
                    "9d303f4b-6293-4a53-9cdb-7142fcf7f6dc",
                    0,
                    datetime(2021, 4, 7, 1, 1, 13, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "http://drm.moha.gov.np/layers/",
                    "e0ec0d4c-6664-4472-99d4-dbe1a5ec9d40",
                    "web app",
                    "95982e94-8567-491c-bee5-33e562f4897a",
                    None,
                    datetime(2015, 4, 29, 21, 54, 15, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "http://shapefiles.fews.net/west-africa201307.zip",
                    "f37c3d54-41c5-45e6-a7fb-64031ceadcc3",
                    "shp",
                    "c74d1089-08a2-487c-a27e-bcf309013c9b",
                    None,
                    datetime(2015, 10, 14, 19, 5, 28, tzinfo=timezone.utc),
                    "",
                ),
            ]
            netlocs = dataset_processor.get_netlocs()
            assert sorted(netlocs) == [
                "drive.google.com",
                "drm.moha.gov.np",
                "shapefiles.fews.net",
            ]
