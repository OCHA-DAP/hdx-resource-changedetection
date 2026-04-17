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
                6,
            ),
        )
