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
            "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5",
            357102,
            datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
            "1234",
            False,
        )
        resources = {"1a2b": resource}
        broken_resource = (
            "https://test.com/myfile.xlsx",
            "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5",
            "xlsx",
            "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5",
            357102,
            datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
            "1234",
            True,
        )
        broken_resources = {"1a2b": broken_resource}
        result = [
            357102,
            "Sun, 10 Nov 2019 08:04:26 GMT",
            "1234",
            200,
        ]
        results_input = {"1a2b": result}
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["nothing: 1a2b"])
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(datasets_to_revise, {})

        # Although the size has changed the http modified is the same as
        # the resource last_modified so we populate it with today instead
        result[0] = 357103
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["size|today: 1a2b"])
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {
                        "size": 357103,
                        # "last_modified": "2019-11-10T08:04:27",  # hash has not changed - don't update
                    },
                }
            },
        )

        # today < resource date so don't change resource date
        today = datetime(2019, 11, 9, 8, 4, 27, tzinfo=timezone.utc)
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["size: 1a2b"])
        check.equal(broken_output, [])

        today = datetime(2019, 11, 10, 8, 4, 27, tzinfo=timezone.utc)
        result[0] = 357102
        result[1] = "Sun, 10 Nov 2019 08:04:27 GMT"
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["modified: 1a2b"])
        check.equal(broken_output, [])

        result[1] = "Sun, 10 Nov 2019 08:04:25 GMT"
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["modified http<resource: 1a2b"])
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(datasets_to_revise, {})

        # Although the etag has changed the http modified is the same as
        # the resource last_modified so we populate it with today instead
        result[1] = "Sun, 10 Nov 2019 08:04:26 GMT"
        result[2] = "1235"
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["etag|today: 1a2b"])
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {
                        "hash": "1235",
                        "last_modified": "2019-11-10T08:04:27",
                    },
                }
            },
        )

        result[1] = "Sun, 10 Nov 2019 08:04:28 GMT"
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["etag|modified: 1a2b"])
        check.equal(broken_output, [])

        result[0] = 357103
        result[2] = "1234"
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["size|modified: 1a2b"])
        check.equal(broken_output, [])

        result[0] = None
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["no size|modified: 1a2b"])
        check.equal(broken_output, [])

        result[2] = None
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["no etag|no size|modified: 1a2b"])
        check.equal(broken_output, [])

        result[1] = None
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["no etag|no size|no modified: 1a2b"])
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(datasets_to_revise, {})

        result[3] = 0
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["no hash|no size|no modified: 1a2b"])
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(datasets_to_revise, {})

        result[2] = "1234"
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["no size|no modified: 1a2b"])
        check.equal(broken_output, [])

        # The hash has changed and there is no http modified so we use today as
        # modified
        result[2] = "1235"
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["hash|no size|no modified|today: 1a2b"])
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {
                        "hash": "1235",
                        "last_modified": "2019-11-10T08:04:27",
                    },
                }
            },
        )

        result[2] = None
        result[3] = 403
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["FORBIDDEN  no hash|no size|no modified: 1a2b"])
        check.equal(broken_output, ["FORBIDDEN: 1a2b"])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {"broken_link": True},
                }
            },
        )

        result[3] = 429
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(
            change_output,
            ["TOO_MANY_REQUESTS  no hash|no size|no modified: 1a2b"],
        )
        check.equal(broken_output, ["TOO_MANY_REQUESTS: 1a2b"])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(datasets_to_revise, {})

        result[3] = 410
        results = Results(today, results_input, broken_resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["GONE|wontrevise  no hash|no size|no modified: 1a2b"])
        check.equal(broken_output, ["GONE|wontrevise: 1a2b"])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {  # resource already broken
                # "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                #     "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                #     "update__resources__1a2b": {"broken_link": True},
                # }
            },
        )

        result[3] = 504
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(
            change_output,
            ["GATEWAY_TIMEOUT  no hash|no size|no modified: 1a2b"],
        )
        check.equal(broken_output, ["GATEWAY_TIMEOUT: 1a2b"])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {"broken_link": True},
                }
            },
        )

        resource = (
            "https://test.com/myfile.xlsx",
            "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5",
            "xlsx",
            "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5",
            357102,
            None,
            "1234",
            False,
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
        change_output, broken_output = results.output()
        check.equal(change_output, ["etag|today: 1a2b"])
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {
                        "hash": "1235",
                        "last_modified": "2019-11-10T08:04:27",
                    },
                }
            },
        )

        # No resource modified and http modified is set so use it since etag
        # has changed. Don't use today as we have a new http modified value.
        result[1] = "Sun, 10 Nov 2019 08:04:26 GMT"
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["etag|modified: 1a2b"])
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {
                        "hash": "1235",
                        "last_modified": "2019-11-10T08:04:26",
                    },
                }
            },
        )

        result[2] = "1234"
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["modified: 1a2b"])
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                # "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                #     "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                #     "update__resources__1a2b": {
                #         "last_modified": "2019-11-10T08:04:26"  # hash has not changed - don't update
                #     },
                # }
            },
        )

        result[3] = -101
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["UNSPECIFIED SERVER ERROR: 1a2b"])
        check.equal(broken_output, ["UNSPECIFIED SERVER ERROR: 1a2b"])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {"broken_link": True},
                }
            },
        )

        result[3] = -11
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["TOO LARGE TO HASH: 1a2b"])
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(datasets_to_revise, {})

        result[3] = -1
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["MIMETYPE != HDX FORMAT  modified: 1a2b"])
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                # "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                #     "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                #     "update__resources__1a2b": {
                #         "last_modified": "2019-11-10T08:04:26"  # hash has not changed - don't update
                #     },
                # }
            },
        )

        result[2] = "1235"
        result[3] = -2
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["SIGNATURE != HDX FORMAT  hash|modified: 1a2b"])
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {
                        "hash": "1235",
                        "last_modified": "2019-11-10T08:04:26",
                    },
                }
            },
        )

        result[0] = 357103
        result[3] = -3
        results = Results(today, results_input, resources)
        results.process()
        change_output, broken_output = results.output()
        check.equal(change_output, ["SIZE != HTTP SIZE  hash|size|modified: 1a2b"])
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {
                        "hash": "1235",
                        "size": 357103,
                        "last_modified": "2019-11-10T08:04:26",
                    },
                }
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
        change_output, broken_output = results.output()
        check.equal(
            change_output,
            ["SIGNATURE != HDX FORMAT  hash|size|modified: 6"],
        )
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {
                        "hash": "1235",
                        "size": 357103,
                        "last_modified": "2019-11-10T08:04:26",
                    },
                    "update__resources__1a3b": {
                        "hash": "1235",
                        "size": 357103,
                        "last_modified": "2019-11-10T08:04:26",
                    },
                    "update__resources__1a4b": {
                        "hash": "1235",
                        "size": 357103,
                        "last_modified": "2019-11-10T08:04:26",
                    },
                    "update__resources__1a5b": {
                        "hash": "1235",
                        "size": 357103,
                        "last_modified": "2019-11-10T08:04:26",
                    },
                    "update__resources__1a6b": {
                        "hash": "1235",
                        "size": 357103,
                        "last_modified": "2019-11-10T08:04:26",
                    },
                    "update__resources__1a7b": {
                        "hash": "1235",
                        "size": 357103,
                        "last_modified": "2019-11-10T08:04:26",
                    },
                }
            },
        )

        resource2 = (
            "https://test2.com/myfile.xlsx",
            "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b6",
            "xlsx",
            "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e6",
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
        change_output, broken_output = results.output()
        check.equal(
            change_output,
            [
                "SIGNATURE != HDX FORMAT  hash|size|modified: 1a2b, 1a3b, 1a4b",
                "SIGNATURE != HDX FORMAT  modified http<resource: 1a5b, 1a6b, 1a7b",
            ],
        )
        check.equal(broken_output, [])
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {
                        "hash": "1235",
                        "size": 357103,
                        "last_modified": "2019-11-10T08:04:26",
                    },
                    "update__resources__1a3b": {
                        "hash": "1235",
                        "size": 357103,
                        "last_modified": "2019-11-10T08:04:26",
                    },
                    "update__resources__1a4b": {
                        "hash": "1235",
                        "size": 357103,
                        "last_modified": "2019-11-10T08:04:26",
                    },
                }
            },
        )
