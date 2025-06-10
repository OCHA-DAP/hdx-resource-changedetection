from datetime import datetime, timezone

from pytest_check import check

from hdx.resource.changedetection.head_results import HeadResults


class TestHeadResults:
    def test_headresults(self):
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
        head_results = HeadResults(results_input, resources)
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "Y",
                    "ETag Changed": "N",
                    "New Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "Y",
                    "Size Changed": "N",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                }
            },
        )
        datasets_to_revise = head_results.get_datasets_to_revise()
        check.equal(datasets_to_revise, {})
        resources_to_get = head_results.get_distributed_resources_to_get()
        check.equal(resources_to_get, [])

        result[0] = 357103
        head_results = HeadResults(results_input, resources)
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "Y",
                    "ETag Changed": "N",
                    "New Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "Y",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                }
            },
        )
        datasets_to_revise = head_results.get_datasets_to_revise()
        check.equal(datasets_to_revise, {})
        resources_to_get = head_results.get_distributed_resources_to_get()
        check.equal(
            resources_to_get,
            [
                (
                    "https://test.com/myfile.xlsx",
                    "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5",
                    "xlsx",
                    "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5",
                    357102,
                    datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
                    "1234",
                    False,
                )
            ],
        )

        result[0] = 357102
        result[1] = "Sun, 10 Nov 2019 08:04:27 GMT"
        head_results = HeadResults(results_input, resources)
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "Y",
                    "ETag Changed": "N",
                    "New Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "",  # will get resource as etag should have changed
                    "New Size": "Y",
                    "Size Changed": "N",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                }
            },
        )

        result[1] = "Sun, 10 Nov 2019 08:04:25 GMT"
        head_results = HeadResults(results_input, resources)
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "Y",
                    "ETag Changed": "N",
                    "New Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "N",
                    "Modified Value": "",
                    "New Size": "Y",
                    "Size Changed": "N",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                }
            },
        )
        datasets_to_revise = head_results.get_datasets_to_revise()
        check.equal(datasets_to_revise, {})
        resources_to_get = head_results.get_distributed_resources_to_get()
        check.equal(resources_to_get, [])

        result[1] = "Sun, 10 Nov 2019 08:04:26 GMT"
        result[2] = "1235"
        head_results = HeadResults(results_input, resources)
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "Y",
                    "ETag Changed": "Y",
                    "New Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "Y",
                    "Size Changed": "N",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "Y",
                }
            },
        )
        datasets_to_revise = head_results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {"hash": "1235"},
                }
            },
        )
        resources_to_get = head_results.get_distributed_resources_to_get()
        check.equal(resources_to_get, [])

        result[1] = "Sun, 10 Nov 2019 08:04:27 GMT"
        head_results = HeadResults(results_input, resources)
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "Y",
                    "ETag Changed": "Y",
                    "New Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "http",
                    "New Size": "Y",
                    "Size Changed": "N",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "Y",
                }
            },
        )

        result[0] = 357103
        result[2] = "1234"
        head_results = HeadResults(results_input, resources)
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "Y",
                    "ETag Changed": "N",
                    "New Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "",
                    "New Size": "Y",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                }
            },
        )

        result[0] = None
        head_results = HeadResults(results_input, resources)
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "Y",
                    "ETag Changed": "N",
                    "New Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "",
                    "New Size": "N",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                }
            },
        )
        resources_to_get = head_results.get_distributed_resources_to_get()
        check.equal(
            resources_to_get,
            [
                (
                    "https://test.com/myfile.xlsx",
                    "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5",
                    "xlsx",
                    "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5",
                    357102,
                    datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
                    "1234",
                    False,
                )
            ],
        )

        result[2] = None
        head_results = HeadResults(results_input, resources)
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "N",
                    "ETag Changed": "Y",
                    "New Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "",
                    "New Size": "N",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                }
            },
        )
        resources_to_get = head_results.get_distributed_resources_to_get()
        check.equal(len(resources_to_get), 1)

        result[1] = None
        head_results = HeadResults(results_input, resources)
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "N",
                    "ETag Changed": "Y",
                    "New Modified": "N",
                    "Modified Changed": "Y",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "N",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                }
            },
        )
        resources_to_get = head_results.get_distributed_resources_to_get()
        check.equal(
            resources_to_get,
            [
                (
                    "https://test.com/myfile.xlsx",
                    "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5",
                    "xlsx",
                    "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5",
                    357102,
                    datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
                    "1234",
                    False,
                )
            ],
        )

        result[3] = 403
        head_results = HeadResults(results_input, resources)
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "FORBIDDEN",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "",
                    "ETag Changed": "",
                    "New Modified": "",
                    "Modified Changed": "",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "",
                    "Size Changed": "",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                }
            },
        )
        datasets_to_revise = head_results.get_datasets_to_revise()
        check.equal(datasets_to_revise, {})
        resources_to_get = head_results.get_distributed_resources_to_get()
        check.equal(
            resources_to_get,
            [
                (
                    "https://test.com/myfile.xlsx",
                    "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5",
                    "xlsx",
                    "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5",
                    357102,
                    datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
                    "1234",
                    False,
                )
            ],
        )

        result[3] = 429
        head_results = HeadResults(results_input, resources)
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "TOO_MANY_REQUESTS",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "",
                    "ETag Changed": "",
                    "New Modified": "",
                    "Modified Changed": "",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "",
                    "Size Changed": "",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                }
            },
        )
        datasets_to_revise = head_results.get_datasets_to_revise()
        check.equal(datasets_to_revise, {})
        resources_to_get = head_results.get_distributed_resources_to_get()
        check.equal(
            resources_to_get,
            [
                (
                    "https://test.com/myfile.xlsx",
                    "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5",
                    "xlsx",
                    "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5",
                    357102,
                    datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
                    "1234",
                    False,
                )
            ],
        )

        result[3] = 410
        head_results = HeadResults(results_input, resources)
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "Y",
                    "Head Status": "GONE",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "",
                    "ETag Changed": "",
                    "New Modified": "",
                    "Modified Changed": "",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "",
                    "Size Changed": "",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                }
            },
        )
        datasets_to_revise = head_results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {"broken_link": True},
                }
            },
        )
        resources_to_get = head_results.get_distributed_resources_to_get()
        check.equal(resources_to_get, [])

        result[3] = 504
        head_results = HeadResults(results_input, resources)
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "Y",
                    "Head Status": "GATEWAY_TIMEOUT",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "",
                    "ETag Changed": "",
                    "New Modified": "",
                    "Modified Changed": "",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "",
                    "Size Changed": "",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                }
            },
        )
        datasets_to_revise = head_results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {"broken_link": True},
                }
            },
        )

        result[3] = -101
        head_results = HeadResults(results_input, broken_resources)
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "Y",
                    "Set Broken": "N",
                    "Head Status": "UNSPECIFIED SERVER ERROR",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "",
                    "ETag Changed": "",
                    "New Modified": "",
                    "Modified Changed": "",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "",
                    "Size Changed": "",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                }
            },
        )
        datasets_to_revise = head_results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {  # resource already broken
                # "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                #     "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                #     "update__resources__1a2b": {"broken_link": True},
                # }
            },
        )
        resources_to_get = head_results.get_distributed_resources_to_get()
        check.equal(resources_to_get, [])

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
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "N",
                    "ETag Changed": "Y",
                    "New Modified": "N",
                    "Modified Changed": "Y",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "N",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                },
                "1a3b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "N",
                    "ETag Changed": "Y",
                    "New Modified": "N",
                    "Modified Changed": "Y",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "N",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                },
                "1a4b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "N",
                    "ETag Changed": "Y",
                    "New Modified": "N",
                    "Modified Changed": "Y",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "N",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                },
                "1a5b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "N",
                    "ETag Changed": "Y",
                    "New Modified": "N",
                    "Modified Changed": "Y",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "N",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                },
                "1a6b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "N",
                    "ETag Changed": "Y",
                    "New Modified": "N",
                    "Modified Changed": "Y",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "N",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                },
                "1a7b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "N",
                    "ETag Changed": "Y",
                    "New Modified": "N",
                    "Modified Changed": "Y",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "N",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                },
            },
        )
        resources_to_get = head_results.get_distributed_resources_to_get()
        check.equal(len(resources_to_get), 6)
        check.equal(resources_to_get[0], resource)

        resource2 = (
            "https://test2.com/myfile.xlsx",
            "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b6",
            "xlsx",
            "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e6",
            357103,
            datetime(2019, 11, 10, 8, 4, 27, tzinfo=timezone.utc),
            "1235",
            False,
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
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "N",
                    "ETag Changed": "Y",
                    "New Modified": "N",
                    "Modified Changed": "Y",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "N",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                },
                "1a3b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "N",
                    "ETag Changed": "Y",
                    "New Modified": "N",
                    "Modified Changed": "Y",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "N",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                },
                "1a4b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "N",
                    "ETag Changed": "Y",
                    "New Modified": "N",
                    "Modified Changed": "Y",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "N",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                },
                "1a5b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "N",
                    "ETag Changed": "Y",
                    "New Modified": "N",
                    "Modified Changed": "Y",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "N",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                },
                "1a6b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "N",
                    "ETag Changed": "Y",
                    "New Modified": "N",
                    "Modified Changed": "Y",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "N",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                },
                "1a7b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "N",
                    "ETag Changed": "Y",
                    "New Modified": "N",
                    "Modified Changed": "Y",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "New Size": "N",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "N",
                },
            },
        )
        resources_to_get = head_results.get_distributed_resources_to_get()
        check.equal(len(resources_to_get), 6)
        check.equal(resources_to_get[0], resource)
        check.equal(resources_to_get[1], resource2)
        netlocs = head_results.get_netlocs()
        check.equal(sorted(netlocs), ["test.com", "test2.com"])
        datasets_to_revise = head_results.get_datasets_to_revise()
        check.equal(datasets_to_revise, {})

        resource2 = (
            "https://test2.com/myfile.xlsx",
            "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b6",
            "xlsx",
            "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e6",
            None,
            None,
            "",
            False,
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
        resource_status = {}
        head_results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "N",
                    "Existing Modified": "N",
                    "Existing Size": "N",
                    "Existing Broken": "N",
                    "Set Broken": "N",
                    "Head Status": "OK",
                    "Head Error": "",
                    "Get Status": "",
                    "Get Error": "",
                    "New ETag": "Y",
                    "ETag Changed": "Y",
                    "New Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "http",
                    "New Size": "Y",
                    "Size Changed": "Y",
                    "New Hash": "",
                    "Hash Changed": "",
                    "Update": "Y",
                }
            },
        )
        datasets_to_revise = head_results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e6": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e6"},
                    "update__resources__1a2b": {
                        "hash": "1234",
                        "size": 357102,
                        "last_modified": "2019-11-10T08:04:26",
                    },
                }
            },
        )
