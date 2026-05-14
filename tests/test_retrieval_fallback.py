"""
Unit tests for the retrieval class fallback statuses (111-115).
"""

from unittest.mock import AsyncMock

import pytest
from pytest_check import check

from hdx.resource.changedetection.retrieval import Retrieval


class TestRetrieveFallback:
    @pytest.mark.parametrize(
        "internal_status, expected_final_status",
        [
            (1, 111),  # md5-nozip-asfb
            (2, 112),  # md5-excel-asfb
            (3, 113),  # crc-all-asfb
            (4, 114),  # md5-fb-asfb
            (5, 115),  # md5-zip-asfb
        ],
    )
    def test_retrieval_fallback_statuses(
        self, mocker, internal_status, expected_final_status
    ):
        """
        Tests the fallback mechanisms (111-115) triggered when try_crc = True
        but the asynchronous CRC calculation fails, forcing a full download on a new connection.
        """
        # Using a live URL that naturally returns `Accept-Ranges: bytes` and a zip signature
        # so we pass the initial CRC gate without needing to mock the aiohttp response object.
        url = "https://data.humdata.org/dataset/ea1d4259-040c-40bd-b1b3-eee9caadf9d3/resource/12b59516-5839-48ce-a13e-3e6d5455dc43/download/pcn_children_under_five_2020_geotiff.zip"
        urls = [("1", "id_1", url, "zip", None, None, None, "N")]

        # 1. Force try_crc = True by faking a file size larger than CRC_SIZE_THRESHOLD
        mocker.patch(
            "hdx.resource.changedetection.retrieval.get_http_size",
            return_value=50_000_000,
        )

        # 2. Force the CRC check to fail (returns empty string), dropping us into the fallback block
        mocker.patch(
            "hdx.resource.changedetection.retrieval.Retrieval.get_async_crc_sum",
            new_callable=AsyncMock,
            return_value="",
        )

        # 3. Mock the full file hash to return our target internal status instantly.
        # This completely bypasses the actual streaming download, keeping the test lightning fast.
        mocker.patch(
            "hdx.resource.changedetection.retrieval.Retrieval.hash_full_file",
            new_callable=AsyncMock,
            return_value=("dummy_fallback_hash", internal_status, 50_000_000),
        )

        res = Retrieval("test", {"data.humdata.org"}).retrieve(urls)

        # Verify the fallback modifier (110) was successfully added to the internal status
        check.equal(res["id_1"][8], expected_final_status)
