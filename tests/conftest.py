from os.path import join
from urllib.parse import urlsplit

import pytest
from hdx.api.configuration import Configuration
from hdx.utilities.path import script_dir_plus_file
from hdx.utilities.useragent import UserAgent

from hdx.resource.changedetection.cli import run_cli


def pytest_addoption(parser):
    """
    Register a command-line option (--run-redis) for running tests that need redis.
    """
    parser.addoption(
        "--run-redis",
        action="store_true",
        default=False,
        help="Run tests marked as needing redis",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "needs_redis: mark test as needing redis to run")


def pytest_runtest_setup(item):
    """
    This function is invoked before each test is executed. It checks if a test
    is marked with 'needs_redis' and skips it unless the '--run-redis' flag is provided.
    """
    if "needs_redis" in item.keywords and not item.config.getoption("--run-redis"):
        pytest.skip(
            "Skipping redis test. Use '--run-redis' to run tests that need redis."
        )


@pytest.fixture(scope="session")
def urls():
    url1 = "https://www.fogcam.org/"
    url2 = "https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/notfound"  # 404 not found
    url3 = "file://lala:10"
    url4 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/COD_MOZ_Admin0.geojson"
    url5 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/hotosm_nic_airports_lines_shp.zip"
    url6 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/Dataset.csv"
    url7 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/list_one.xls"
    url8 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/ACLED-Country-Coverage-and-ISO-Codes_8.2019.xlsx"
    url9 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/response.html"
    url10 = "https://s3.us-east-1.amazonaws.com/hdx-production-filestore/resources/864ef944-a6a8-496a-b0f7-94ab37e34922/mli-admbnda-adm1-gov.geojson?response-content-disposition=attachment%3B%20filename%3D%22mli-admbnda-adm1-gov.geojson%22&AWSAccessKeyId=AKIAXYC32WNAR4IVQZNB&Signature=UAVT4jY%2BY85qjV8zrUYZpYyk7jw%3D&Expires=1741114494"  # 403 forbidden
    url11 = "https://docs.google.com/spreadsheets/d/1_HfS24hr5dcM3nRalWcwrV1wQMIu0-BEuQrLecniLok/export?format=csv"  # 410 gone
    url12 = "https://gdacs-smcs.unosat.org/events/view/id/19"  # connection timeout
    url13 = "http://drm.moha.gov.np/layers/"  # misformatted domain name
    url14 = "https://drive.google.com/uc?export=download&id=1yK5olSSLBKmfE0T_EgTP_d8l-lTFfz1I"  # hashed
    url15 = "https://drive.google.com/uc?export=download&id=139nYi36M_m8WsUCAIetOEbMAvQIv-YG3"  # hashed
    url16 = "https://raw.githubusercontent.com/OCHA-DAP/hdx-python-utilities/refs/heads/main/tests/fixtures/file_hashing/bad_index.xlsx"
    url17 = "https://data.humdata.org/dataset/bcf8a4bb-5253-4065-ae34-6e8cdcb90d57/resource/fbadf0c3-ff99-4cad-91a6-010d3b028e84/download/daily_tmax_cnt_tmaxgt30c_01.zip"
    return [
        ("1", "1", url1, "html", None, None, None, "N"),
        ("2", "2", url2, "csv", None, None, None, "N"),
        ("3", "3", url3, "csv", None, None, None, "N"),
        ("4", "4", url4, "geojson", None, None, None, "N"),
        ("5", "5", url5, "shp", None, None, None, "N"),
        ("6", "6", url6, "csv", None, None, None, "N"),
        ("7", "7", url7, "xls", None, None, None, "N"),
        ("8", "8", url8, "xlsx", None, None, None, "N"),
        ("9", "9", url9, "html", None, None, None, "N"),
        ("10", "10", url9, "csv", None, None, None, "N"),
        ("11", "11", url9, "xls", None, None, None, "N"),
        ("12", "12", url10, "geojson", None, None, None, "N"),
        ("13", "13", url11, "csv", None, None, None, "N"),
        ("14", "14", url12, "csv", None, None, None, "N"),
        ("15", "15", url13, "geojson", None, None, None, "N"),
        ("16", "16", url14, "csv", None, None, None, "N"),
        ("17", "17", url15, "xlsx", None, None, None, "N"),
        ("18", "18", url16, "xlsx", None, None, None, "N"),
        ("19", "19", url17, "zip", None, None, None, "N"),
    ]


@pytest.fixture(scope="session")
def netlocs(urls):
    return {urlsplit(x[0]).netloc for x in urls}


@pytest.fixture(scope="session")
def configuration():
    UserAgent.set_global("test")
    Configuration._create(
        hdx_read_only=True,
        hdx_site="prod",
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), run_cli
        ),
    )
    return Configuration.read()


@pytest.fixture(scope="session")
def folder():
    return join("tests", "fixtures")
