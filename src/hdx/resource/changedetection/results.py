import logging
from datetime import datetime
from http import HTTPStatus

from hdx.utilities.dateparse import parse_date

from .utilities import get_blank_log_status, revise_resource, status_lookup

logger = logging.getLogger(__name__)


class Results:
    def __init__(
        self,
        today: datetime,
        results: dict[str, tuple],
        resources: dict[str, tuple],
    ) -> None:
        self._today = today
        self._results = results
        self._resources = resources
        self._datasets_to_revise = {}

    def add_more_results(self, results: dict[str, tuple], resources: dict[str, tuple]):
        self._results.update(results)
        self._resources.update(resources)

    def process(self, resource_status: dict[str, dict]) -> None:
        for resource_id, result in self._results.items():
            log_status = get_blank_log_status()
            resource = self._resources[resource_id]
            existing_hash = resource[6]
            if existing_hash:
                log_status["Existing Hash"] = "Y"
            else:
                log_status["Existing Hash"] = "N"
            existing_size = resource[4]
            if existing_size:
                log_status["Existing Size"] = "Y"
            else:
                log_status["Existing Size"] = "N"
            resource_date = resource[5]
            if resource_date:
                log_status["Existing Modified"] = "Y"
            else:
                log_status["Existing Modified"] = "N"
            existing_broken = resource[7]
            if existing_broken:
                log_status["Existing Broken"] = "Y"
            else:
                log_status["Existing Broken"] = "N"
            dataset_id = resource[3]
            (
                size,
                last_modified,
                etag,
                final_hash,
                sig_match,
                mime_match,
                size_match,
                http_status,
                status,
            ) = result

            match status:
                case -1:
                    log_status["Error"] = "Too big, no etag"
                case -10:
                    log_status["Error"] = "ClientResponseError"
                case -11:
                    log_status["Error"] = "Unknown Error"

            log_status["HTTP Status"] = status_lookup[http_status]

            if status <= 0:
                if (
                    http_status != HTTPStatus.TOO_MANY_REQUESTS and not existing_broken
                ):  # currently broken
                    resource_info = {"broken_link": True}
                    revise_resource(
                        self._datasets_to_revise, dataset_id, resource_id, resource_info
                    )
                    log_status["Set Broken"] = "Y"
                resource_status[resource_id] = log_status
                continue

            resource_info = {}
            update = False
            hash_changed = False

            if sig_match is None:
                log_status["Sig Match"] = ""
            else:
                log_status["Sig Match"] = "Y" if sig_match else "N"
            if mime_match is None:
                log_status["Mime Match"] = ""
            else:
                log_status["Mime Match"] = "Y" if mime_match else "N"
            if size_match is None:
                log_status["Size Match"] = ""
            else:
                log_status["Size Match"] = "Y" if size_match else "N"
            log_status["Has ETag"] = "Y" if etag else "N"

            if final_hash:
                match status:
                    case 1:
                        log_status["Hash Type"] = "md5"
                    case 2:
                        log_status["Hash Type"] = "md5-xl"
                    case 3:
                        log_status["Hash Type"] = "crc"
                    case 4:
                        log_status["Hash Type"] = "md5-fb"
                    case 5:
                        log_status["Hash Type"] = "etag"
                    case 6:
                        log_status["Hash Type"] = "etag-sz"
                    case 7:
                        log_status["Hash Type"] = "crc-as"
                    case _:
                        log_status["Hash Type"] = ""
                log_status["Has Hash"] = "Y"
                if final_hash != resource[6]:
                    if log_status["Hash Type"]:
                        resource_info["hash"] = final_hash
                        hash_changed = True
                        update = True
                    log_status["Hash Changed"] = "Y"
                else:
                    log_status["Hash Changed"] = "N"
            else:
                log_status["Has Hash"] = "N"
                if resource[6]:
                    log_status["Hash Changed"] = "Y"
                else:
                    log_status["Hash Changed"] = "N"
            if size:
                log_status["Has Size"] = "Y"
                if size != resource[4]:
                    log_status["Size Changed"] = "Y"
                    resource_info["size"] = size
                    update = True
                else:
                    log_status["Size Changed"] = "N"
            else:
                log_status["Has Size"] = "N"
                if resource[4]:
                    log_status["Size Changed"] = "Y"
                else:
                    log_status["Size Changed"] = "N"

            resource_date = resource[5]
            if last_modified:
                log_status["Has Modified"] = "Y"
                last_modified = parse_date(last_modified)
                if not resource_date or last_modified > resource_date:
                    log_status["Modified Changed"] = "Y"
                    log_status["Modified Newer"] = "Y"
                    # Only update if hash has also changed
                    if hash_changed:
                        update = True
                elif last_modified < resource_date:
                    log_status["Modified Changed"] = "Y"
                    log_status["Modified Newer"] = "N"
                else:
                    log_status["Modified Changed"] = "N"
            else:
                log_status["Has Modified"] = "N"
                if resource_date:
                    log_status["Modified Changed"] = "Y"
                else:
                    log_status["Modified Changed"] = "N"

            if update:
                if not last_modified or (
                    resource_date and last_modified <= resource_date
                ):
                    if resource_date:
                        if self._today > resource_date:
                            last_modified = self._today
                            log_status["Modified Value"] = "today"
                        else:
                            last_modified = resource_date
                    else:
                        last_modified = self._today
                        log_status["Modified Value"] = "today"
                else:
                    log_status["Modified Value"] = "http"
                # Only update last modified if hash has changed
                if hash_changed and last_modified and last_modified != resource_date:
                    dt_notz = last_modified.replace(tzinfo=None)
                    resource_info["last_modified"] = dt_notz.isoformat()

                if resource_info:
                    revise_resource(
                        self._datasets_to_revise,
                        dataset_id,
                        resource_id,
                        resource_info,
                    )
                    log_status["Update"] = "Y"
            resource_status[resource_id] = log_status

    def get_datasets_to_revise(self) -> dict[str, dict]:
        return self._datasets_to_revise
