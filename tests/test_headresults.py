from datetime import datetime, timezone

from pytest_check import check

from hdx.resource.changedetection.head_results import HeadResults


class TestHeadResults:
    def test_headresults(self):
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
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["nothing: 1a2b"])
        check.equal(get_output, [])

        result[0] = 357103
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["size: 1a2b"])
        check.equal(get_output, ["size: 1a2b"])

        result[0] = 357102
        result[1] = "Sun, 10 Nov 2019 08:04:27 GMT"
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["modified: 1a2b"])
        check.equal(get_output, ["modified: 1a2b"])

        result[1] = "Sun, 10 Nov 2019 08:04:25 GMT"
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["modified: http<resource: 1a2b"])
        check.equal(get_output, [])

        result[1] = "Sun, 10 Nov 2019 08:04:26 GMT"
        result[2] = "1235"
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["etag: 1a2b"])
        check.equal(get_output, [])

        result[1] = "Sun, 10 Nov 2019 08:04:27 GMT"
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["etag|modified: 1a2b"])
        check.equal(get_output, [])

        result[0] = 357103
        result[2] = "1234"
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["size|modified: 1a2b"])
        check.equal(get_output, ["size|modified: 1a2b"])

        result[0] = None
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["no size|modified: 1a2b"])
        check.equal(get_output, ["modified: 1a2b"])

        result[2] = None
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["no etag|no size|modified: 1a2b"])
        check.equal(get_output, ["no etag|modified: 1a2b"])

        result[1] = None
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["no etag|no size|no modified: 1a2b"])
        check.equal(get_output, ["no etag: 1a2b"])

        result[3] = 403
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["FORBIDDEN: 1a2b"])
        check.equal(get_output, ["FORBIDDEN: 1a2b"])

        result[3] = 429
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["TOO_MANY_REQUESTS: 1a2b"])
        check.equal(get_output, ["TOO_MANY_REQUESTS: 1a2b"])

        result[3] = 410
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["GONE: 1a2b"])
        check.equal(get_output, [])

        result[3] = 504
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["GATEWAY_TIMEOUT: 1a2b"])
        check.equal(get_output, [])

        result[3] = -100
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["UNSPECIFIED SERVER ERROR: 1a2b"])
        check.equal(get_output, [])

        resources = {
            "1a2b": resource,
            "1a3b": resource,
            "1a4b": resource,
            "1a5b": resource,
            "1a6b": resource,
            "1a7b": resource,
        }
        results_input = {
            "1a2b": result,
            "1a3b": result,
            "1a4b": result,
            "1a5b": result,
            "1a6b": result,
            "1a7b": result,
        }
        result[3] = 200
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["no etag|no size|no modified: 6"])
        check.equal(get_output, ["no etag: 6"])

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
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["no etag|no size|no modified: 6"])
        check.equal(get_output, ["no etag: 6"])
        resources_to_get = head_results.get_distributed_resources_to_get()
        check.equal(len(resources_to_get), 6)
        check.equal(resources_to_get[0], resource)
        check.equal(resources_to_get[1], resource2)
        netlocs = head_results.get_netlocs()
        check.equal(sorted(netlocs), ["test.com", "test2.com"])

        resource2 = (
            "https://test2.com/myfile.xlsx",
            "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b6",
            "xlsx",
            None,
            None,
            "",
        )
        resources = {"1a2b": resource2}
        result = [
            357102,
            "Sun, 10 Nov 2019 08:04:26 GMT",
            "1234",
            200,
        ]
        results_input = {"1a2b": result}
        head_results = HeadResults(results_input, resources)
        head_results.process()
        change_output, get_output = head_results.output()
        check.equal(change_output, ["etag|size|modified: 1a2b"])
        check.equal(get_output, [])
        resources_to_update = head_results.get_resources_to_update()
        check.equal(
            resources_to_update,
            {
                "1a2b": (
                    357102,
                    datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
                    "1234",
                )
            },
        )
