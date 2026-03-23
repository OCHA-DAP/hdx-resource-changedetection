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
                4862,
                "Tue, 23 Mar 2021 17:04:04 GMT",
                None,
                "59f2f6123cfb7859ef71ba4cf8bcce12",
                None,
                None,
                True,
                200,
                1,
            ),
        )
        check.equal(result["2"], (None, None, None, None, None, None, None, 404, -10))
        check.equal(result["3"], (None, None, None, None, None, None, None, -101, -11))
        check.equal(
            result["4"],
            (
                596488,
                "Thu, 05 Mar 2026 03:01:36 GMT",
                '"69a8f210-91a08"',
                '"69a8f210-91a08"',
                True,
                True,
                None,
                200,
                5,
            ),
        )
        check.equal(
            result["5"],
            (
                8461,
                "Thu, 05 Mar 2026 03:01:36 GMT",
                '"69a8f210-210d"',
                "914bd375",
                True,
                True,
                True,
                200,
                3,
            ),
        )
        check.equal(
            result["6"],
            (
                26084,
                "Thu, 05 Mar 2026 03:01:36 GMT",
                '"69a8f210-65e4"',
                '"69a8f210-65e4"',
                None,
                True,
                None,
                200,
                5,
            ),
        )
        check.equal(
            result["7"],
            (
                81920,
                "Thu, 05 Mar 2026 03:01:36 GMT",
                '"69a8f210-14000"',
                '"69a8f210-14000"',
                True,
                True,
                None,
                200,
                5,
            ),
        )
        check.equal(
            result["8"],
            (
                20984,
                "Thu, 05 Mar 2026 03:01:36 GMT",
                '"69a8f210-51f8"',
                "08157f501860340ef6e102e9037d1da2",
                True,
                True,
                True,
                200,
                2,
            ),
        )
        check.equal(
            result["9"],
            (
                2017,
                "Thu, 05 Mar 2026 03:01:36 GMT",
                '"69a8f210-7e1"',
                '"69a8f210-7e1"',
                None,
                None,
                None,
                200,
                5,
            ),
        )
        check.equal(
            result["10"],
            (
                2017,
                "Thu, 05 Mar 2026 03:01:36 GMT",
                '"69a8f210-7e1"',
                '"69a8f210-7e1"',
                None,
                False,
                None,
                200,
                5,
            ),
        )
        check.equal(
            result["11"],
            (
                2017,
                "Thu, 05 Mar 2026 03:01:36 GMT",
                '"69a8f210-7e1"',
                '"69a8f210-7e1"',
                False,
                False,
                None,
                200,
                5,
            ),
        )
        check.equal(
            result["12"],
            (None, None, None, None, None, None, None, 403, -10),
        )
        check.equal(
            result["13"],
            (None, None, None, None, None, None, None, 410, -10),
        )
        check.equal(
            result["14"],
            (None, None, None, None, None, None, None, -101, -11),
        )
        check.equal(
            result["15"],
            (None, None, None, None, None, None, None, -101, -11),
        )
        check.equal(
            result["16"],
            (
                30776,
                "Thu, 11 Mar 2021 17:21:43 GMT",
                None,
                "abfe9f3bada6c937e8103e215c826451",
                None,
                None,
                True,
                200,
                1,
            ),
        )
        check.equal(
            result["17"],
            (
                1787826,
                "Thu, 27 Jan 2022 21:30:41 GMT",
                None,
                "ca81ea9804b0c9882e2df7f9034cd1fe",
                True,
                None,
                True,
                200,
                2,
            ),
        )
        check.equal(
            result["18"],
            (
                2830,
                None,
                '"8b4e815cc6bd3fb47121497fc2344422d5f9da2108f10c356c0d04607a141ca7"',
                "376c95f3d2bc02ac814f5b08815fbfac",
                True,
                None,
                True,
                200,
                4,
            ),
        )
        check.equal(
            result["19"],
            (
                90846092,
                "Wed, 17 Dec 2025 18:30:23 GMT",
                '"a7a6ecb7730590d1ff73a1d5e621cb89"',
                "ca7df19a",
                True,
                True,
                None,
                200,
                7,
            ),
        )
