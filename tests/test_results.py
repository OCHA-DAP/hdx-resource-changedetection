from datetime import datetime, timezone

from pytest_check import check

from hdx.resource.changedetection.results import Results


class TestResults:
    def test_results(self):
        today = datetime(2019, 11, 10, 8, 4, 27, tzinfo=timezone.utc)
        resource = (
            "https://test.com/myfile.xlsx",
            "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5",
            "xlsx",
            357102,
            datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
            "1234",
        )
        resources = {"1a2b": resource}
        result = [
            357102,
            "Sun, 10 Nov 2019 08:04:26 GMT",
            "1234",
            200,
        ]
        results_input = {"1a2b": result}
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["nothing: 1a2b"])

        # Although the size has changed the http modified is the same as
        # the resource last_modified so we populate it with today instead
        result[0] = 357103
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["size|today: 1a2b"])

        # today < resource date so don't change resource date
        today = datetime(2019, 11, 9, 8, 4, 27, tzinfo=timezone.utc)
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["size: 1a2b"])

        today = datetime(2019, 11, 10, 8, 4, 27, tzinfo=timezone.utc)
        result[0] = 357102
        result[1] = "Sun, 10 Nov 2019 08:04:27 GMT"
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["modified: 1a2b"])

        result[1] = "Sun, 10 Nov 2019 08:04:25 GMT"
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["modified: http<resource: 1a2b"])

        # Although the etag has changed the http modified is the same as
        # the resource last_modified so we populate it with today instead
        result[1] = "Sun, 10 Nov 2019 08:04:26 GMT"
        result[2] = "1235"
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["etag|today: 1a2b"])
        resources_to_update = results.get_resources_to_update()
        check.equal(
            resources_to_update,
            {
                "1a2b": (
                    357102,
                    datetime(2019, 11, 10, 8, 4, 27, tzinfo=timezone.utc),
                    "1235",
                )
            },
        )

        result[1] = "Sun, 10 Nov 2019 08:04:28 GMT"
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["etag|modified: 1a2b"])

        result[0] = 357103
        result[2] = "1234"
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["size|modified: 1a2b"])

        result[0] = None
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["no size|modified: 1a2b"])

        result[2] = None
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["no etag|no size|modified: 1a2b"])

        result[1] = None
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["no etag|no size|no modified: 1a2b"])
        resources_to_update = results.get_resources_to_update()
        check.equal(resources_to_update, {})

        result[3] = 0
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["no hash|no size|no modified: 1a2b"])
        resources_to_update = results.get_resources_to_update()
        check.equal(resources_to_update, {})

        result[2] = "1234"
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["no size|no modified: 1a2b"])

        # The hash has changed and there is no http modified so we use today as
        # modified
        result[2] = "1235"
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["hash|no size|no modified|today: 1a2b"])
        resources_to_update = results.get_resources_to_update()
        check.equal(
            resources_to_update,
            {
                "1a2b": (
                    None,
                    datetime(2019, 11, 10, 8, 4, 27, tzinfo=timezone.utc),
                    "1235",
                )
            },
        )

        result[2] = None
        result[3] = 403
        results = Results(today, results_input, resources)
        results.process()
        check.equal(
            results.output(), ["FORBIDDEN  no hash|no size|no modified: 1a2b"]
        )

        result[3] = 429
        results = Results(today, results_input, resources)
        results.process()
        check.equal(
            results.output(),
            ["TOO_MANY_REQUESTS  no hash|no size|no modified: 1a2b"],
        )

        result[3] = 410
        results = Results(today, results_input, resources)
        results.process()
        check.equal(
            results.output(), ["GONE  no hash|no size|no modified: 1a2b"]
        )
        resources_to_update = results.get_resources_to_update()
        check.equal(resources_to_update, {})

        result[3] = 504
        results = Results(today, results_input, resources)
        results.process()
        check.equal(
            results.output(),
            ["GATEWAY_TIMEOUT  no hash|no size|no modified: 1a2b"],
        )

        resource = (
            "https://test.com/myfile.xlsx",
            "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5",
            "xlsx",
            357102,
            None,
            "1234",
        )
        resources = {"1a2b": resource}
        result = [
            357102,
            None,
            "1235",
            200,
        ]

        # No resource modified or http modified but etag has changed so use
        # today as modified.
        results_input = {"1a2b": result}
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["etag|today: 1a2b"])
        resources_to_update = results.get_resources_to_update()
        check.equal(
            resources_to_update,
            {
                "1a2b": (
                    357102,
                    datetime(2019, 11, 10, 8, 4, 27, tzinfo=timezone.utc),
                    "1235",
                )
            },
        )

        # No resource modified and http modified is set so use it since etag
        # has changed. Don't use today as we have a new http modified value.
        result[1] = "Sun, 10 Nov 2019 08:04:26 GMT"
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["etag|modified: 1a2b"])
        resources_to_update = results.get_resources_to_update()
        check.equal(
            resources_to_update,
            {
                "1a2b": (
                    357102,
                    datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
                    "1235",
                )
            },
        )

        result[2] = "1234"
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["modified: 1a2b"])

        result[3] = -100
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["UNSPECIFIED SERVER ERROR: 1a2b"])

        result[3] = -99
        results = Results(today, results_input, resources)
        results.process()
        check.equal(results.output(), ["TOO LARGE TO HASH: 1a2b"])
        resources_to_update = results.get_resources_to_update()
        check.equal(resources_to_update, {})

        result[3] = -1
        results = Results(today, results_input, resources)
        results.process()
        check.equal(
            results.output(), ["MIMETYPE != HDX FORMAT  modified: 1a2b"]
        )

        result[2] = "1235"
        result[3] = -2
        results = Results(today, results_input, resources)
        results.process()
        check.equal(
            results.output(), ["SIGNATURE != HDX FORMAT  hash|modified: 1a2b"]
        )
        resources_to_update = results.get_resources_to_update()
        check.equal(
            resources_to_update,
            {
                "1a2b": (
                    357102,
                    datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
                    "1235",
                )
            },
        )

        result[0] = 357103
        result[3] = -3
        results = Results(today, results_input, resources)
        results.process()
        check.equal(
            results.output(), ["SIZE != HTTP SIZE  hash|size|modified: 1a2b"]
        )
        resources_to_update = results.get_resources_to_update()
        check.equal(
            resources_to_update,
            {
                "1a2b": (
                    357103,
                    datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
                    "1235",
                )
            },
        )

        resources = {
            "1a2b": resource,
            "1a3b": resource,
            "1a4b": resource,
            "1a5b": resource,
            "1a6b": resource,
            "1a7b": resource,
        }
        result[3] = -2
        results_input = {
            "1a2b": result,
            "1a3b": result,
            "1a4b": result,
            "1a5b": result,
            "1a6b": result,
            "1a7b": result,
        }
        results = Results(today, results_input, resources)
        results.process()
        check.equal(
            results.output(),
            ["SIGNATURE != HDX FORMAT  hash|size|modified: 6"],
        )

        resource2 = (
            "https://test2.com/myfile.xlsx",
            "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b6",
            "xlsx",
            357103,
            datetime(2019, 11, 10, 8, 4, 27, tzinfo=timezone.utc),
            "1235",
        )
        resources = {
            "1a2b": resource,
            "1a3b": resource,
            "1a4b": resource,
            "1a5b": resource2,
            "1a6b": resource2,
            "1a7b": resource2,
        }
        results = Results(today, results_input, resources)
        results.process()
        check.equal(
            results.output(),
            [
                "SIGNATURE != HDX FORMAT  hash|size|modified: 1a2b, 1a3b, 1a4b",
                "SIGNATURE != HDX FORMAT  modified: http<resource: 1a5b, 1a6b, 1a7b",
            ],
        )
