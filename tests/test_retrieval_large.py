"""
Unit tests for the retrieval class.

"""

from pytest_check import check

from hdx.resource.changedetection.retrieval import Retrieval


class TestRetrieveLarge:
    def test_retrieval_large(self):
        urls = [
            (
                "1",
                "1",
                "https://data.worldpop.org/repo/wopr/UKR/v1/unconstrained/100m/ukr_agesex_2020_100m_unconstrained_v1_0.zip",
                "zipped tiff",
                None,
                None,
                None,
                "N",
            )
        ]
        result = Retrieval(
            "test",
            {"https://data.worldpop.org/"},
        ).retrieve(urls)
        check.equal(
            result["1"],
            (
                14075521786,
                "Tue, 15 Mar 2022 10:13:01 GMT",
                '"346f76afa-5da3f0ac43490"',
                '"346f76afa-5da3f0ac43490"',
                True,
                True,
                None,
                200,
                101,
            ),
        )

    def test_retrieval_status_5_large_zip(self, mocker):
        urls = [
            (
                "1",
                "status_5_zip_id",
                "https://data.humdata.org/dataset/0474df44-62b5-4a4c-a4fd-fd733979e2cc/resource/358825e9-bdac-4fa0-ae0e-07ec3f2fd1b4/download/idn_children_under_five_2020_geotiff.zip",
                "zip",
                None,
                None,
                None,
                "N",
            )
        ]

        # By pretending the server didn't provide a Content-Length,
        # we bypass the CRC check and force a full streaming download (Status 5).
        mocker.patch(
            "hdx.resource.changedetection.retrieval.get_http_size", return_value=None
        )

        result = Retrieval(
            "test",
            {"data.humdata.org"},
        ).retrieve(urls)

        res = result["status_5_zip_id"]

        # res[7] is HTTP Status (200 OK)
        # res[8] is the internal code (5 = streamed md5-zip)
        check.equal(res[7], 200)
        check.equal(res[8], 5)

    def test_retrieval_status_minus_1_too_large(self, mocker):
        """Status -1: File exceeds MAX_DOWNLOAD_SIZE."""
        url = "https://data.humdata.org/dataset/0474df44-62b5-4a4c-a4fd-fd733979e2cc/resource/358825e9-bdac-4fa0-ae0e-07ec3f2fd1b4/download/idn_children_under_five_2020_geotiff.zip"
        urls = [("1", "id_1", url, "zip", None, None, None, "N")]

        # Drop the max download threshold to 10 bytes to guarantee a size violation.
        mocker.patch("hdx.resource.changedetection.retrieval.MAX_DOWNLOAD_SIZE", 10)

        # Raise the CRC threshold so we don't accidentally fall into the CRC block.
        mocker.patch(
            "hdx.resource.changedetection.retrieval.CRC_SIZE_THRESHOLD", 500_000_000
        )

        res = Retrieval("test", {"data.humdata.org"}).retrieve(urls)
        check.equal(res["id_1"][8], -1)

    def test_retrieval_status_11_large_etag_no_crc(self, mocker):
        """Status 11: File is larger than ETAG_SIZE_THRESHOLD but skips CRC check."""
        url = "https://data.humdata.org/dataset/00dd6f4c-4888-443e-b589-b64a60d855a3/resource/354378a7-7c98-428f-80b6-d89ee600604a/download/projections-demographiques.xlsx"
        urls = [("1", "id_1", url, "xlsx", None, None, None, "N")]

        # Fake the file size to be 500MB (above the 419MB ETag threshold)
        mocker.patch(
            "hdx.resource.changedetection.retrieval.get_http_size",
            return_value=500_000_000,
        )

        res = Retrieval("test", {"data.humdata.org"}).retrieve(urls)
        check.equal(res["id_1"][8], 11)
