"""
Unit tests for the retrieval class.

"""

from pytest_check import check

from hdx.resource.changedetection.head_retrieval import HeadRetrieval


class TestHeadRetrieve:
    def test_retrieval(self):
        url1 = "https://itcorp.com"
        url2 = "https://github.com/mcarans/hdx-data-freshness/raw/d1616d76c3b6b8ef5029eb6964b93cde688efd53/tests/fixtures/day0/notfound"
        url3 = "file://lala:10"
        url4 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/COD_MOZ_Admin0.geojson"
        url5 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/hotosm_nic_airports_lines_shp.zip"
        url6 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/Dataset.csv"
        url7 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/list_one.xls"
        url8 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/ACLED-Country-Coverage-and-ISO-Codes_8.2019.xlsx"
        url9 = "https://ocha-dap.github.io/hdx-data-freshness/tests/fixtures/retrieve/response.html"
        urls = [
            (url1, "1", "html"),
            (url2, "2", "csv"),
            (url3, "3", "csv"),
            (url4, "4", "geojson"),
            (url5, "5", "shp"),
            (url6, "6", "csv"),
            (url7, "7", "xls"),
            (url8, "8", "xlsx"),
            (url9, "9", "html"),
            (url9, "10", "csv"),
            (url9, "11", "xls"),
        ]
        result = HeadRetrieval(
            "test",
            {"itcorp.com", "github.com", "ocha-dap.github.io", "lala:10"},
        ).retrieve(urls)
        check.equal(
            result["1"],
            (
                "859",
                "Sat, 20 Dec 2014 12:44:12 GMT",
                '"35b-50aa52ea9f4dc"',
                200,
            ),
        )
        check.equal(result["2"], (None, None, None, 404))
        check.equal(result["3"], (None, None, None, -100))
        check.equal(
            result["4"],
            (
                "176643",
                "Tue, 12 Mar 2024 03:15:06 GMT",
                'W/"65efc8ba-91a08"',
                200,
            ),
        )
        check.equal(
            result["5"],
            ("8461", "Tue, 12 Mar 2024 03:15:06 GMT", '"65efc8ba-210d"', 200),
        )
        check.equal(
            result["6"],
            (
                "11169",
                "Tue, 12 Mar 2024 03:15:06 GMT",
                'W/"65efc8ba-65e4"',
                200,
            ),
        )
        check.equal(
            result["7"],
            (
                "81920",
                "Tue, 12 Mar 2024 03:15:06 GMT",
                '"65efc8ba-14000"',
                200,
            ),
        )
        check.equal(
            result["8"],
            ("20984", "Tue, 12 Mar 2024 03:15:06 GMT", '"65efc8ba-51f8"', 200),
        )
        check.equal(
            result["9"],
            ("871", "Tue, 12 Mar 2024 03:15:06 GMT", 'W/"65efc8ba-7e1"', 200),
        )
        check.equal(
            result["10"],
            ("871", "Tue, 12 Mar 2024 03:15:06 GMT", 'W/"65efc8ba-7e1"', 200),
        )
        check.equal(
            result["11"],
            ("871", "Tue, 12 Mar 2024 03:15:06 GMT", 'W/"65efc8ba-7e1"', 200),
        )
