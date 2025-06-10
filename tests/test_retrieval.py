"""
Unit tests for the retrieval class.

"""

from pytest_check import check

from hdx.resource.changedetection.retrieval import Retrieval


class TestRetrieve:
    def test_retrieval(self, urls, netlocs):
        result = Retrieval(
            "test",
            netlocs,
        ).retrieve(urls)
        check.equal(result["1"], (None, None, '"cx4nhab5wy1nfr"', 200))
        check.equal(result["2"], (None, None, None, 404))
        check.equal(result["3"], (None, None, None, -101))
        check.equal(
            result["4"],
            (
                None,
                "Mon, 19 May 2025 23:55:10 GMT",
                'W/"682bc4de-91a08"',
                200,
            ),
        )
        check.equal(
            result["5"],
            (8461, "Mon, 19 May 2025 23:55:10 GMT", '"682bc4de-210d"', 200),
        )
        check.equal(
            result["6"],
            (
                None,
                "Mon, 19 May 2025 23:55:10 GMT",
                'W/"682bc4de-65e4"',
                200,
            ),
        )
        check.equal(
            result["7"],
            (
                81920,
                "Mon, 19 May 2025 23:55:10 GMT",
                '"682bc4de-14000"',
                200,
            ),
        )
        check.equal(
            result["8"],
            (20984, "Mon, 19 May 2025 23:55:10 GMT", '"682bc4de-51f8"', 200),
        )
        check.equal(
            result["9"],
            (None, "Mon, 19 May 2025 23:55:10 GMT", 'W/"682bc4de-7e1"', 200),
        )
        check.equal(
            result["10"],
            (None, "Mon, 19 May 2025 23:55:10 GMT", 'W/"682bc4de-7e1"', 200),
        )
        check.equal(
            result["11"],
            (None, "Mon, 19 May 2025 23:55:10 GMT", 'W/"682bc4de-7e1"', 200),
        )
        check.equal(
            result["12"],
            (None, None, None, 403),
        )
        check.equal(
            result["13"],
            (None, None, None, 410),
        )
        check.equal(
            result["14"],
            (None, None, None, -101),
        )
        check.equal(
            result["15"],
            (None, None, None, -101),
        )
        check.equal(
            result["16"],
            (
                30776,
                "Thu, 11 Mar 2021 17:21:43 GMT",
                "abfe9f3bada6c937e8103e215c826451",
                0,
            ),
        )
        check.equal(
            result["17"],
            (
                1787826,
                "Thu, 27 Jan 2022 21:30:41 GMT",
                "09a4dd3b41c52e17a70b3b6635d6a3af",
                0,
            ),
        )
