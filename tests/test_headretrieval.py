"""
Unit tests for the retrieval class.

"""

from pytest_check import check

from hdx.resource.changedetection.head_retrieval import HeadRetrieval


class TestHeadRetrieve:
    def test_retrieval(self, urls, netlocs):
        result = HeadRetrieval(
            "test",
            netlocs,
        ).retrieve(urls)
        check.equal(result["1"], (None, None, 'W/"cx4nhab5wy1nfr"', 200))
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
            (30776, "Thu, 11 Mar 2021 17:21:43 GMT", None, 200),
        )
        check.equal(
            result["17"],
            (1787826, "Thu, 27 Jan 2022 21:30:41 GMT", None, 200),
        )
