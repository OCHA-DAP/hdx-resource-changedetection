from datetime import datetime, timezone

from pytest_check import check

from hdx.resource.changedetection.results import Results


class TestResults:
    def test_results(self):
        today = datetime(2019, 11, 10, 8, 4, 27, tzinfo=timezone.utc)
        resource = (
            "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5",
            "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5",
            "https://test.com/myfile.xlsx",
            "xlsx",
            "1234",
            357102,
            datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
            False,
        )
        resources = {"1a2b": resource}
        broken_resource = (
            "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5",
            "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5",
            "https://test.com/myfile.xlsx",
            "xlsx",
            "1234",
            357102,
            datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
            True,
        )
        broken_resources = {"1a2b": broken_resource}
        result = [
            357102,
            "Sun, 10 Nov 2019 08:04:26 GMT",
            "4867",
            "1234",
            True,
            True,
            True,
            200,
            2,
        ]
        results_input = {"1a2b": result}
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "N",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "md5-xl",
                    "Update": "N",
                    "Error": "",
                }
            },
        )
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(datasets_to_revise, {})

        result[0] = 357103  # size changed
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        # Although the size has changed the http modified is the same as
        # the resource last_modified so we populate it with today instead
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "today",
                    "Has Size": "Y",
                    "Size Changed": "Y",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "md5-xl",
                    "Update": "Y",
                    "Error": "",
                }
            },
        )
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {"size": 357103, "broken_link": False},
                    # hash has not changed - don't update modified
                }
            },
        )

        result[0] = 357102
        result[1] = "Sun, 10 Nov 2019 08:04:27 GMT"  # date changed
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        # today < resource date so don't change resource date
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "N",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "md5-xl",
                    "Update": "N",
                    "Error": "",
                }
            },
        )

        result[1] = "Sun, 10 Nov 2019 08:04:25 GMT"  # older modified
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "N",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "N",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "md5-xl",
                    "Update": "N",
                    "Error": "",
                }
            },
        )
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(datasets_to_revise, {})

        result[1] = "Sun, 10 Nov 2019 08:04:26 GMT"
        result[2] = "4868"
        result[3] = "4868"
        result[8] = 5  # using etag which changed
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        # Although the etag has changed the http modified is the same as
        # the resource last_modified so we populate it with today instead
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "today",
                    "Has Size": "Y",
                    "Size Changed": "N",
                    "Has Hash": "Y",
                    "Hash Changed": "Y",
                    "Hash Type": "etag",
                    "Update": "Y",
                    "Error": "",
                }
            },
        )
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {
                        "hash": "4868",
                        "last_modified": "2019-11-10T08:04:27",
                        "broken_link": False,
                    },
                }
            },
        )

        result[1] = "Sun, 10 Nov 2019 08:04:27 GMT"  # newer modified date
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "http",
                    "Has Size": "Y",
                    "Size Changed": "N",
                    "Has Hash": "Y",
                    "Hash Changed": "Y",
                    "Hash Type": "etag",
                    "Update": "Y",
                    "Error": "",
                }
            },
        )

        result[0] = 357103  # size changed
        result[2] = "4867"  # etag unchanged
        result[3] = "1234"  # hash unchanged
        # modified is still newer
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "http",
                    "Has Size": "Y",
                    "Size Changed": "Y",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "etag",
                    "Update": "Y",
                    "Error": "",
                }
            },
        )

        result[0] = None  # no size
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "",
                    "Has Size": "N",
                    "Size Changed": "Y",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "etag",
                    "Update": "N",
                    "Error": "",
                }
            },
        )

        result[2] = None  # no etag
        result[3] = None  # no hash
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "N",
                    "Has Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "",
                    "Has Size": "N",
                    "Size Changed": "Y",
                    "Has Hash": "N",
                    "Hash Changed": "Y",
                    "Hash Type": "",
                    "Update": "N",
                    "Error": "",
                }
            },
        )

        result[1] = None  # no date
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Set Broken": "N",
                    "Size Match": "Y",
                    "Has ETag": "N",
                    "Has Modified": "N",
                    "Modified Changed": "Y",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "N",
                    "Size Changed": "Y",
                    "Has Hash": "N",
                    "Hash Changed": "Y",
                    "Hash Type": "",
                    "Update": "N",
                    "Error": "",
                }
            },
        )

        # The hash has changed and there is no http modified so we use today as
        # modified
        result[2] = "4367"
        result[3] = "1235"
        result[8] = 1
        results = Results(today, results_input, resources)
        resource_status = {"1a2b": {}}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "N",
                    "Modified Changed": "Y",
                    "Modified Newer": "",
                    "Modified Value": "today",
                    "Has Size": "N",
                    "Size Changed": "Y",
                    "Has Hash": "Y",
                    "Hash Changed": "Y",
                    "Hash Type": "md5",
                    "Update": "Y",
                    "Error": "",
                }
            },
        )
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {
                        "hash": "1235",
                        "last_modified": "2019-11-10T08:04:27",
                        "broken_link": False,
                    },
                }
            },
        )

        result[2] = None  # No etag
        result[3] = None  # No hash
        result[4] = None  # No signature check
        result[5] = None  # No mimetype check
        result[6] = None  # No size check
        result[7] = 403  # HTTP status forbidden
        result[8] = -10  # ClientResponseError
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "FORBIDDEN",
                    "Sig Match": "",
                    "Mime Match": "",
                    "Size Match": "",
                    "Set Broken": "Y",
                    "Has ETag": "",
                    "Has Modified": "",
                    "Modified Changed": "",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "",
                    "Size Changed": "",
                    "Has Hash": "",
                    "Hash Changed": "",
                    "Hash Type": "",
                    "Update": "N",
                    "Error": "ClientResponseError",
                }
            },
        )
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

        result[7] = 429  # TOO MANY REQUESTS
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "TOO_MANY_REQUESTS",
                    "Sig Match": "",
                    "Mime Match": "",
                    "Size Match": "",
                    "Set Broken": "N",
                    "Has ETag": "",
                    "Has Modified": "",
                    "Modified Changed": "",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "",
                    "Size Changed": "",
                    "Has Hash": "",
                    "Hash Changed": "",
                    "Hash Type": "",
                    "Update": "N",
                    "Error": "ClientResponseError",
                }
            },
        )
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(datasets_to_revise, {})

        result[7] = 410  # Gone
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "GONE",
                    "Sig Match": "",
                    "Mime Match": "",
                    "Size Match": "",
                    "Set Broken": "Y",
                    "Has ETag": "",
                    "Has Modified": "",
                    "Modified Changed": "",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "",
                    "Size Changed": "",
                    "Has Hash": "",
                    "Hash Changed": "",
                    "Hash Type": "",
                    "Update": "N",
                    "Error": "ClientResponseError",
                }
            },
        )
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

        result[7] = 504  # Gateway timeout
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "GATEWAY_TIMEOUT",
                    "Sig Match": "",
                    "Mime Match": "",
                    "Size Match": "",
                    "Set Broken": "Y",
                    "Has ETag": "",
                    "Has Modified": "",
                    "Modified Changed": "",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "",
                    "Size Changed": "",
                    "Has Hash": "",
                    "Hash Changed": "",
                    "Hash Type": "",
                    "Update": "N",
                    "Error": "ClientResponseError",
                }
            },
        )
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
            "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5",
            "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5",
            "https://test.com/myfile.xlsx",
            "xlsx",
            "1234",
            357102,
            None,
            False,
        )
        resources = {"1a2b": resource}
        result = [
            419430401,
            None,
            "4867",
            "1235",
            True,
            True,
            None,
            200,
            6,
        ]
        results_input = {"1a2b": result}
        results = Results(today, results_input, resources)
        resource_status = {"1a2b": {}}
        results.process(resource_status)
        # No resource modified or http modified but etag has changed so use
        # today as modified.
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "N",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "N",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "today",
                    "Has Size": "Y",
                    "Size Changed": "Y",
                    "Has Hash": "Y",
                    "Hash Changed": "Y",
                    "Hash Type": "etag-sz",
                    "Update": "Y",
                    "Error": "",
                }
            },
        )
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {
                        "hash": "1235",
                        "size": 419430401,
                        "last_modified": "2019-11-10T08:04:27",
                        "broken_link": False,
                    },
                }
            },
        )

        result[1] = "Sun, 10 Nov 2019 08:04:26 GMT"
        result[6] = True
        result[8] = 4  # fallback hash
        results = Results(today, results_input, resources)
        resource_status = {"1a2b": {}}
        results.process(resource_status)
        # No resource modified and http modified is set so use it since hash
        # has changed. Don't use today as we have a new http modified value.
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "N",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "http",
                    "Has Size": "Y",
                    "Size Changed": "Y",
                    "Has Hash": "Y",
                    "Hash Changed": "Y",
                    "Hash Type": "md5-fb",
                    "Update": "Y",
                    "Error": "",
                }
            },
        )
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {
                        "hash": "1235",
                        "size": 419430401,
                        "last_modified": "2019-11-10T08:04:26",
                        "broken_link": False,
                    },
                }
            },
        )

        result[0] = 357102
        result[3] = "1234"
        result[6] = False
        result[8] = 1
        results = Results(today, results_input, resources)
        resource_status = {"1a2b": {}}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "N",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "N",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "N",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "md5",
                    "Update": "N",
                    "Error": "",
                }
            },
        )
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                # hash has not changed - don't update
            },
        )

        result[0] = 36700160  # large zip
        result[6] = None  # won't be reading whole file
        result[8] = 7  # will use CRC with ranges
        results = Results(today, results_input, resources)
        resource_status = {"1a2b": {}}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "N",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "http",
                    "Has Size": "Y",
                    "Size Changed": "Y",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "crc-as",
                    "Update": "Y",
                    "Error": "",
                }
            },
        )
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {"size": 36700160, "broken_link": False},
                }
            },
        )

        result[7] = -101
        result[8] = -11
        results = Results(today, results_input, broken_resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "Y",
                    "HTTP Status": "UNSPECIFIED SERVER ERROR",
                    "Sig Match": "",
                    "Mime Match": "",
                    "Size Match": "",
                    "Set Broken": "N",
                    "Has ETag": "",
                    "Has Modified": "",
                    "Modified Changed": "",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "",
                    "Size Changed": "",
                    "Has Hash": "",
                    "Hash Changed": "",
                    "Hash Type": "",
                    "Update": "N",
                    "Error": "Unknown Error",
                }
            },
        )
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {  # resource already broken
            },
        )

        result[7] = 200
        result[8] = -1  # too big to hash
        results = Results(today, results_input, broken_resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "Y",
                    "HTTP Status": "OK",
                    "Sig Match": "",
                    "Mime Match": "",
                    "Size Match": "",
                    "Set Broken": "N",
                    "Has ETag": "",
                    "Has Modified": "",
                    "Modified Changed": "",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "",
                    "Size Changed": "",
                    "Has Hash": "",
                    "Hash Changed": "",
                    "Hash Type": "",
                    "Update": "N",
                    "Error": "Too big, no etag",
                }
            },
        )

        result[3] = "1235"
        result[4] = False
        result[5] = False
        result[8] = 7
        results = Results(today, results_input, broken_resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "Y",
                    "HTTP Status": "OK",
                    "Sig Match": "N",
                    "Mime Match": "N",
                    "Size Match": "",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "today",
                    "Has Size": "Y",
                    "Size Changed": "Y",
                    "Has Hash": "Y",
                    "Hash Changed": "Y",
                    "Hash Type": "crc-as",
                    "Update": "Y",
                    "Error": "",
                }
            },
        )
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5"},
                    "update__resources__1a2b": {
                        "hash": "1235",
                        "size": 36700160,
                        "last_modified": "2019-11-10T08:04:27",
                        "broken_link": False,
                    },
                }
            },
        )

        result[0] = 357103
        result[4] = None
        result[5] = None
        result[6] = False
        result[8] = 3
        results = Results(today, results_input, resources)
        resource_status = {"1a2b": {}}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "N",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "",
                    "Mime Match": "",
                    "Size Match": "N",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "http",
                    "Has Size": "Y",
                    "Size Changed": "Y",
                    "Has Hash": "Y",
                    "Hash Changed": "Y",
                    "Hash Type": "crc",
                    "Update": "Y",
                    "Error": "",
                }
            },
        )
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
                        "broken_link": False,
                    },
                }
            },
        )

        resource = (
            "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e5",
            "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5",
            "https://test.com/myfile.xlsx",
            "xlsx",
            "1234",
            357102,
            datetime(2019, 11, 10, 8, 4, 26, tzinfo=timezone.utc),
            False,
        )
        resources = {
            "1a2b": resource,
            "1a3b": resource,
            "1a4b": resource,
            "1a5b": resource,
            "1a6b": resource,
            "1a7b": resource,
        }
        result = [
            357102,
            "Sun, 10 Nov 2019 08:04:26 GMT",
            "4867",
            "1234",
            True,
            True,
            True,
            200,
            3,
        ]
        results_input = {
            "1a2b": result,
            "1a3b": result,
            "1a4b": result,
            "1a5b": result,
            "1a6b": result,
            "1a7b": result,
        }
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "N",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "crc",
                    "Update": "N",
                    "Error": "",
                },
                "1a3b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "N",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "crc",
                    "Update": "N",
                    "Error": "",
                },
                "1a4b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "N",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "crc",
                    "Update": "N",
                    "Error": "",
                },
                "1a5b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "N",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "crc",
                    "Update": "N",
                    "Error": "",
                },
                "1a6b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "N",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "crc",
                    "Update": "N",
                    "Error": "",
                },
                "1a7b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "N",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "crc",
                    "Update": "N",
                    "Error": "",
                },
            },
        )

        resource2 = (
            "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e6",
            "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b6",
            "https://test2.com/myfile.xlsx",
            "xlsx",
            "1235",
            357103,
            datetime(2019, 11, 10, 8, 4, 27, tzinfo=timezone.utc),
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
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "N",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "crc",
                    "Update": "N",
                    "Error": "",
                },
                "1a3b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "N",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "crc",
                    "Update": "N",
                    "Error": "",
                },
                "1a4b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "N",
                    "Modified Newer": "",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "N",
                    "Has Hash": "Y",
                    "Hash Changed": "N",
                    "Hash Type": "crc",
                    "Update": "N",
                    "Error": "",
                },
                "1a5b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "N",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "Y",
                    "Has Hash": "Y",
                    "Hash Changed": "Y",
                    "Hash Type": "crc",
                    "Update": "Y",
                    "Error": "",
                },
                "1a6b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "N",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "Y",
                    "Has Hash": "Y",
                    "Hash Changed": "Y",
                    "Hash Type": "crc",
                    "Update": "Y",
                    "Error": "",
                },
                "1a7b": {
                    "Existing Hash": "Y",
                    "Existing Modified": "Y",
                    "Existing Size": "Y",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "Y",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "N",
                    "Modified Value": "",
                    "Has Size": "Y",
                    "Size Changed": "Y",
                    "Has Hash": "Y",
                    "Hash Changed": "Y",
                    "Hash Type": "crc",
                    "Update": "Y",
                    "Error": "",
                },
            },
        )
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e6": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e6"},
                    "update__resources__1a5b": {
                        "hash": "1234",
                        "size": 357102,
                        "broken_link": False,
                    },
                    "update__resources__1a6b": {
                        "hash": "1234",
                        "size": 357102,
                        "broken_link": False,
                    },
                    "update__resources__1a7b": {
                        "hash": "1234",
                        "size": 357102,
                        "broken_link": False,
                    },
                }
            },
        )

        resource2 = (
            "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e6",
            "a8b51b81-1fa7-499d-a9f2-3d0bce06b5b6",
            "https://test2.com/myfile.xlsx",
            "xlsx",
            None,
            None,
            "",
            False,
        )
        resources = {"1a2b": resource2}
        result[6] = False
        results_input = {"1a2b": result}
        results = Results(today, results_input, resources)
        resource_status = {}
        results.process(resource_status)
        check.equal(
            resource_status,
            {
                "1a2b": {
                    "Existing Hash": "N",
                    "Existing Modified": "N",
                    "Existing Size": "N",
                    "Existing Broken": "N",
                    "HTTP Status": "OK",
                    "Sig Match": "Y",
                    "Mime Match": "Y",
                    "Size Match": "N",
                    "Set Broken": "N",
                    "Has ETag": "Y",
                    "Has Modified": "Y",
                    "Modified Changed": "Y",
                    "Modified Newer": "Y",
                    "Modified Value": "http",
                    "Has Size": "Y",
                    "Size Changed": "Y",
                    "Has Hash": "Y",
                    "Hash Changed": "Y",
                    "Hash Type": "crc",
                    "Update": "Y",
                    "Error": "",
                }
            },
        )
        datasets_to_revise = results.get_datasets_to_revise()
        check.equal(
            datasets_to_revise,
            {
                "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e6": {
                    "match": {"id": "5eaf2ecd-0b29-46cd-bddb-9c2317c9b8e6"},
                    "update__resources__1a2b": {
                        "hash": "1234",
                        "size": 357102,
                        "last_modified": "2019-11-10T08:04:26",
                        "broken_link": False,
                    },
                }
            },
        )
