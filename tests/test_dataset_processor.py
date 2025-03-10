from datetime import datetime, timezone
from os.path import join

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
            dataset_processor = DatasetProcessor(configuration)
            datasets = dataset_processor.get_all_datasets()
            dataset_processor.process(datasets)
            resources_to_check = (
                dataset_processor.get_distributed_resources_to_check()
            )
            assert resources_to_check == [
                (
                    "https://data.humdata.org/dataset/0d089fa0-3567-4b01-9c03-39d340ff34e3/resource/76defd41-cca7-4dda-8363-2d2d51d6e877/download/ebola-cases-and-deaths-who-gar-sitrep.xls",
                    "76defd41-cca7-4dda-8363-2d2d51d6e877",
                    "xls",
                    "0d089fa0-3567-4b01-9c03-39d340ff34e3",
                    392704,
                    datetime(2019, 11, 10, 7, 51, 16, tzinfo=timezone.utc),
                    "",
                ),
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
                    "https://data.humdata.org/dataset/0d089fa0-3567-4b01-9c03-39d340ff34e3/resource/a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5/download/ebola_data_db_format.xlsx",
                    "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5",
                    "xlsx",
                    "0d089fa0-3567-4b01-9c03-39d340ff34e3",
                    357102,
                    datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
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
                    "https://data.humdata.org/dataset/0d089fa0-3567-4b01-9c03-39d340ff34e3/resource/c59b5722-ca4b-41ca-a446-472d6d824d01/download/ebola_data_db_format.csv",
                    "c59b5722-ca4b-41ca-a446-472d6d824d01",
                    "csv",
                    "0d089fa0-3567-4b01-9c03-39d340ff34e3",
                    1422467,
                    datetime(2019, 11, 10, 8, 12, 59, tzinfo=timezone.utc),
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
                (
                    "https://data.humdata.org/dataset/464950af-0d57-47ae-a1fa-b4413b0adaf7/resource/400acc9c-e58e-4461-b003-c0602a910c3f/download/hdx_signals_location_metadata.csv",
                    "400acc9c-e58e-4461-b003-c0602a910c3f",
                    "csv",
                    "464950af-0d57-47ae-a1fa-b4413b0adaf7",
                    33512,
                    datetime(2025, 3, 4, 16, 9, 59, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/dataset/464950af-0d57-47ae-a1fa-b4413b0adaf7/resource/49056751-af41-430e-92e8-81c4b0e5b38f/download/hdx_signals.csv",
                    "49056751-af41-430e-92e8-81c4b0e5b38f",
                    "csv",
                    "464950af-0d57-47ae-a1fa-b4413b0adaf7",
                    1785034,
                    datetime(2025, 3, 4, 16, 9, 59, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/dataset/464950af-0d57-47ae-a1fa-b4413b0adaf7/resource/fec6b8ba-1629-455b-b151-7436b83a84e8/download/hdx_signals_data_dictionary.csv",
                    "fec6b8ba-1629-455b-b151-7436b83a84e8",
                    "csv",
                    "464950af-0d57-47ae-a1fa-b4413b0adaf7",
                    2165,
                    datetime(2025, 3, 4, 16, 9, 59, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/dataset/8b99ca18-1e41-4e93-93c9-a19e0274bea8/resource/b9789668-955c-47d6-ac39-c0dc19ca1bbd/download/operational_presence.csv",
                    "b9789668-955c-47d6-ac39-c0dc19ca1bbd",
                    "csv",
                    "8b99ca18-1e41-4e93-93c9-a19e0274bea8",
                    7412398,
                    datetime(2025, 2, 10, 21, 35, 16, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/dataset/79fd9fc3-a1aa-4bc1-b785-afae80a280a7/resource/35e3b412-37a9-4c38-aea6-a992198afb26/download/precipitation_arg.csv",
                    "35e3b412-37a9-4c38-aea6-a992198afb26",
                    "csv",
                    "79fd9fc3-a1aa-4bc1-b785-afae80a280a7",
                    2678200,
                    datetime(2018, 10, 30, 8, 10, 51, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/dataset/c3f15254-021d-44c9-bd41-22b37d927fcf/resource/986394e1-fcb6-44f5-9860-22fa817ea517/download/precipitation_gin.csv",
                    "986394e1-fcb6-44f5-9860-22fa817ea517",
                    "csv",
                    "c3f15254-021d-44c9-bd41-22b37d927fcf",
                    24404,
                    datetime(2018, 10, 30, 8, 7, 52, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/dataset/9b7cd60a-c8a6-493c-b132-8726f676a748/resource/1f62a231-e322-4d00-aa1e-64e04a48ac95/download/conflict_data_dza.csv",
                    "1f62a231-e322-4d00-aa1e-64e04a48ac95",
                    "csv",
                    "9b7cd60a-c8a6-493c-b132-8726f676a748",
                    2138103,
                    datetime(2025, 3, 3, 23, 41, 1, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/dataset/9b7cd60a-c8a6-493c-b132-8726f676a748/resource/a9052a1a-51ba-4757-8091-adfe308e6073/download/qc_conflict_data_dza.csv",
                    "a9052a1a-51ba-4757-8091-adfe308e6073",
                    "csv",
                    "9b7cd60a-c8a6-493c-b132-8726f676a748",
                    102620,
                    datetime(2025, 3, 3, 23, 41, 2, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/dataset/c2a3d7af-66e1-483d-b53a-8ee0ee502632/resource/cb82ca27-8a78-44f6-bf49-346cd2cc783a/download/conflict_data_gha.csv",
                    "cb82ca27-8a78-44f6-bf49-346cd2cc783a",
                    "csv",
                    "c2a3d7af-66e1-483d-b53a-8ee0ee502632",
                    30142,
                    datetime(2025, 3, 3, 23, 43, 19, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/dataset/c2a3d7af-66e1-483d-b53a-8ee0ee502632/resource/f253563c-5496-465e-af6d-575946aa60e8/download/qc_conflict_data_gha.csv",
                    "f253563c-5496-465e-af6d-575946aa60e8",
                    "csv",
                    "c2a3d7af-66e1-483d-b53a-8ee0ee502632",
                    1382,
                    datetime(2025, 3, 3, 23, 43, 20, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/dataset/964af584-53cd-4317-b5ef-8ef7b34e783e/resource/7806775c-af75-497d-8b1b-0cb5e17a1076/download/conflict_data_mda.csv",
                    "7806775c-af75-497d-8b1b-0cb5e17a1076",
                    "csv",
                    "964af584-53cd-4317-b5ef-8ef7b34e783e",
                    33996,
                    datetime(2025, 3, 3, 23, 45, 21, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/dataset/964af584-53cd-4317-b5ef-8ef7b34e783e/resource/e6bf0364-4ebd-45a2-a4ba-8c14983f7c6f/download/qc_conflict_data_mda.csv",
                    "e6bf0364-4ebd-45a2-a4ba-8c14983f7c6f",
                    "csv",
                    "964af584-53cd-4317-b5ef-8ef7b34e783e",
                    1693,
                    datetime(2025, 3, 3, 23, 45, 21, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/dataset/15371dd8-4b57-4fc4-a424-7b77c32513f0/resource/ed445f2c-bb7d-4fea-bccb-15f912b9b4ac/download/conflict_data_ssd.csv",
                    "ed445f2c-bb7d-4fea-bccb-15f912b9b4ac",
                    "csv",
                    "15371dd8-4b57-4fc4-a424-7b77c32513f0",
                    728374,
                    datetime(2025, 3, 3, 23, 47, 11, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/dataset/15371dd8-4b57-4fc4-a424-7b77c32513f0/resource/8d8b504c-ac77-47b9-8f13-0d7d70b297d4/download/qc_conflict_data_ssd.csv",
                    "8d8b504c-ac77-47b9-8f13-0d7d70b297d4",
                    "csv",
                    "15371dd8-4b57-4fc4-a424-7b77c32513f0",
                    25209,
                    datetime(2025, 3, 3, 23, 47, 11, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/hxlproxy/api/data-preview.csv?url=https%3A%2F%2Fraw.githubusercontent.com%2Fnytimes%2Fcovid-19-data%2Fmaster%2Fus-states.csv&filename=us-states.csv",
                    "34450bc6-76e5-49a5-879e-26edfa7b3b27",
                    "csv",
                    "f92954dc-3f5b-407a-80a9-1e178280b0d7",
                    None,
                    datetime(2020, 3, 31, 8, 55, 20, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/hxlproxy/api/data-preview.csv?url=https%3A%2F%2Fraw.githubusercontent.com%2Fnytimes%2Fcovid-19-data%2Fmaster%2Fus-counties.csv&filename=us-counties.csv",
                    "171ede69-6592-46dc-9355-bb65b48ae8fc",
                    "csv",
                    "f92954dc-3f5b-407a-80a9-1e178280b0d7",
                    None,
                    datetime(2020, 3, 31, 8, 55, 17, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/hxlproxy/data/download/us-counties-hxl.csv?dest=data_edit&filter01=sort&sort-tags01=%23date&sort-reverse01=on&tagger-match-all=on&tagger-01-header=date&tagger-01-tag=%23date&tagger-02-header=county&tagger-02-tag=%23adm2%2Bname&tagger-03-header=state&tagger-03-tag=%23adm1%2Bname&tagger-04-header=fips&tagger-04-tag=%23adm2%2Bcode&tagger-05-header=cases&tagger-05-tag=%23affected%2Binfected&tagger-06-header=deaths&tagger-06-tag=%23affected%2Bkilled&header-row=1&url=https%3A%2F%2Fraw.githubusercontent.com%2Fnytimes%2Fcovid-19-data%2Fmaster%2Fus-counties.csv",
                    "b06e93b4-9935-46dc-82ac-2f6a8c6c3ab0",
                    "csv",
                    "f92954dc-3f5b-407a-80a9-1e178280b0d7",
                    None,
                    datetime(2020, 3, 31, 14, 32, 3, tzinfo=timezone.utc),
                    "",
                ),
                (
                    "https://data.humdata.org/hxlproxy/data/download/us-states-hxl.csv?dest=data_edit&filter01=sort&sort-tags01=%23date&sort-reverse01=on&tagger-match-all=on&tagger-01-header=date&tagger-01-tag=%23date&tagger-02-header=state&tagger-02-tag=%23adm1%2Bname&tagger-03-header=fips&tagger-03-tag=%23adm1%2Bcode&tagger-04-header=cases&tagger-04-tag=%23affected%2Binfected&tagger-05-header=deaths&tagger-05-tag=%23affected%2Bkilled&header-row=1&url=https%3A%2F%2Fraw.githubusercontent.com%2Fnytimes%2Fcovid-19-data%2Fmaster%2Fus-states.csv",
                    "7153d0f0-56b4-4282-aabc-3a0695812c0e",
                    "csv",
                    "f92954dc-3f5b-407a-80a9-1e178280b0d7",
                    None,
                    datetime(2020, 3, 31, 14, 32, 7, tzinfo=timezone.utc),
                    "",
                ),
            ]
            netlocs = dataset_processor.get_netlocs()
            assert sorted(netlocs) == [
                "data.humdata.org",
                "drive.google.com",
                "drm.moha.gov.np",
                "shapefiles.fews.net",
            ]
