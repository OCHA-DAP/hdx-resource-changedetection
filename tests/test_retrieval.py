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
        check.equal(
            result["1"],
            (
                859,
                "Sat, 20 Dec 2014 12:44:12 GMT",
                '"35b-50aa52ea9f4dc"',
                200,
            ),
        )
        check.equal(result["2"], (None, None, None, 404))
        check.equal(result["3"], (None, None, None, -101))
        check.equal(
            result["4"],
            (
                176643,
                "Tue, 12 Mar 2024 03:15:06 GMT",
                'W/"65efc8ba-91a08"',
                200,
            ),
        )
        check.equal(
            result["5"],
            (8461, "Tue, 12 Mar 2024 03:15:06 GMT", '"65efc8ba-210d"', 200),
        )
        check.equal(
            result["6"],
            (
                11169,
                "Tue, 12 Mar 2024 03:15:06 GMT",
                'W/"65efc8ba-65e4"',
                200,
            ),
        )
        check.equal(
            result["7"],
            (
                81920,
                "Tue, 12 Mar 2024 03:15:06 GMT",
                '"65efc8ba-14000"',
                200,
            ),
        )
        check.equal(
            result["8"],
            (20984, "Tue, 12 Mar 2024 03:15:06 GMT", '"65efc8ba-51f8"', 200),
        )
        check.equal(
            result["9"],
            (871, "Tue, 12 Mar 2024 03:15:06 GMT", 'W/"65efc8ba-7e1"', 200),
        )
        check.equal(
            result["10"],
            (871, "Tue, 12 Mar 2024 03:15:06 GMT", 'W/"65efc8ba-7e1"', 200),
        )
        check.equal(
            result["11"],
            (871, "Tue, 12 Mar 2024 03:15:06 GMT", 'W/"65efc8ba-7e1"', 200),
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
                "aa22983cc9d5a474fd2c77068a747c0e",
                0,
            ),
        )
