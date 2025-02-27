"""Utility to download and hash resources. Uses asyncio. Note that the purpose of
asyncio is to help with IO-bound rather than CPU-bound code (for which multiprocessing
is more suitable as it leverages multiple CPUs). Asyncio allows you to structure your
code so that when one piece of linear single-threaded code (coroutine) is waiting for
something to happen another can take over and use the CPU. While conceptually similar to
threading, the difference is that with asyncio, it is the task of the developer rather
than the OS to decide when to switch to the next task.
"""

import asyncio
import hashlib
import logging
from io import BytesIO
from timeit import default_timer as timer
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlsplit

import aiohttp
from aiolimiter import AsyncLimiter
from openpyxl import load_workbook
from tenacity import (
    retry,
    retry_if_exception_type,
    wait_exponential,
)
from tqdm.asyncio import tqdm_asyncio

logger = logging.getLogger(__name__)


class Retrieval:
    """Retrieval class for downloading and hashing resources.

    Args:
        user_agent (str): User agent string to use when downloading
        url_ignore (Optional[str]): Parts of url to ignore for special xlsx handling
    """

    toolargeerror = "File too large to hash!"
    notmatcherror = "does not match HDX format"
    clienterror_regex = ".Client(.*)Error "
    ignore_mimetypes = ["application/octet-stream", "application/binary"]
    mimetypes = {
        "json": ["application/json"],
        "geojson": ["application/json", "application/geo+json"],
        "shp": ["application/zip", "application/x-zip-compressed"],
        "csv": ["text/csv", "application/zip", "application/x-zip-compressed"],
        "xls": ["application/vnd.ms-excel"],
        "xlsx": [
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ],
    }
    signatures = {
        "json": [b"[", b" [", b"{", b" {"],
        "geojson": [b"[", b" [", b"{", b" {"],
        "shp": [b"PK\x03\x04"],
        "xls": [b"\xd0\xcf\x11\xe0"],
        "xlsx": [b"PK\x03\x04"],
    }

    def __init__(
        self,
        user_agent: str,
        netlocs: Set[str],
        url_ignore: Optional[str] = None,
    ) -> None:
        self._user_agent = user_agent
        self._url_ignore: Optional[str] = url_ignore
        self._rate_limiters = {
            netloc: AsyncLimiter(2, 1) for netloc in netlocs
        }

    @retry(
        retry=retry_if_exception_type(
            (aiohttp.ClientConnectionError, asyncio.TimeoutError)
        ),
        wait=wait_exponential(multiplier=4, min=5, max=30),
    )
    async def fetch(
        self,
        metadata: Tuple,
        session: aiohttp.ClientSession,
    ) -> Tuple:
        """Asynchronous code to download a resource and hash it. Returns a tuple with
        resource information including hashes.

        Args:
            metadata (Tuple): Resource to be checked
            session (Union[aiohttp.ClientSession, RateLimiter]): session to use for requests

        Returns:
            Tuple: Resource information including hash
        """
        url = metadata[0]
        resource_id = metadata[1]
        resource_format = metadata[2]
        size = metadata[3]
        last_modified = metadata[4]
        hash = metadata[5]

        host = urlsplit(url).netloc

        async with self._rate_limiters[host]:
            async with session.get(url) as response:
                headers = response.headers
                mimetype = headers.get("Content-Type")

                try:
                    iterator = response.content.iter_any()
                    first_chunk = await iterator.__anext__()
                    signature = first_chunk[:4]
                    if (
                        resource_format == "xlsx"
                        and mimetype == self.mimetypes["xlsx"][0]
                        and signature == self.signatures["xlsx"][0]
                        and (
                            self._url_ignore not in url
                            if self._url_ignore
                            else True
                        )
                    ):
                        xlsxbuffer = bytearray(first_chunk)
                    else:
                        xlsxbuffer = None
                    md5hash = hashlib.md5(first_chunk)
                    async for chunk in iterator:
                        if chunk:
                            md5hash.update(chunk)
                            if xlsxbuffer:
                                xlsxbuffer.extend(chunk)
                    if xlsxbuffer:
                        workbook = load_workbook(
                            filename=BytesIO(xlsxbuffer), read_only=True
                        )
                        xlsx_md5hash = hashlib.md5()
                        for sheet_name in workbook.sheetnames:
                            sheet = workbook[sheet_name]
                            for cols in sheet.iter_rows(values_only=True):
                                xlsx_md5hash.update(bytes(str(cols), "utf-8"))
                        workbook.close()
                        xlsxbuffer = None
                    else:
                        xlsx_md5hash = None
                    err = None
                    if mimetype not in self.ignore_mimetypes:
                        expected_mimetypes = self.mimetypes.get(
                            resource_format
                        )
                        if expected_mimetypes is not None:
                            if not any(
                                x in mimetype for x in expected_mimetypes
                            ):
                                err = f"File mimetype {mimetype} {self.notmatcherror} {resource_format}!"
                    expected_signatures = self.signatures.get(resource_format)
                    if expected_signatures is not None:
                        found = False
                        for expected_signature in expected_signatures:
                            if (
                                signature[: len(expected_signature)]
                                == expected_signature
                            ):
                                found = True
                                break
                        if not found:
                            sigerr = f"File signature {signature} {self.notmatcherror} {resource_format}!"
                            if err is None:
                                err = sigerr
                            else:
                                err = f"{err} {sigerr}"
                    return (
                        resource_id,
                        url,
                        resource_format,
                        err,
                        md5hash.hexdigest(),
                        xlsx_md5hash.hexdigest() if xlsx_md5hash else None,
                    )
                except Exception as exc:
                    try:
                        code = exc.code
                    except AttributeError:
                        code = ""
                    err = f"Exception during hashing: code={code} message={exc} raised={exc.__class__.__module__}.{exc.__class__.__qualname__} url={url}"
                    raise aiohttp.ClientResponseError(
                        code=code,
                        message=err,
                        request_info=response.request_info,
                        history=response.history,
                    ) from exc

    async def check_urls(
        self, resources_to_check: List[Tuple]
    ) -> Dict[str, Tuple]:
        """Asynchronous code to download resources and hash them. Return dictionary with
        resources information including hashes.

        Args:
            resources_to_check (List[Tuple]): List of resources to be checked
            loop (uvloop.Loop): Event loop to use

        Returns:
            Dict[str, Tuple]: Resources information including hashes
        """
        tasks = []

        conn = aiohttp.TCPConnector(limit=100, limit_per_host=1)
        timeout = aiohttp.ClientTimeout(
            total=60 * 60, sock_connect=30, sock_read=30
        )
        async with aiohttp.ClientSession(
            connector=conn,
            timeout=timeout,
            headers={"User-Agent": self._user_agent},
        ) as session:
            for metadata in resources_to_check:
                task = self.fetch(metadata, session)
                tasks.append(task)
            responses = {}
            for f in tqdm_asyncio.as_completed(tasks, total=len(tasks)):
                (
                    resource_id,
                    url,
                    resource_format,
                    err,
                    http_last_modified,
                    hash,
                    hash_xlsx,
                ) = await f
                responses[resource_id] = (
                    url,
                    resource_format,
                    err,
                    http_last_modified,
                    hash,
                    hash_xlsx,
                )
            return responses

    def retrieve(self, resources_to_check: List[Tuple]) -> Dict[str, Tuple]:
        """Download resources and hash them. Return dictionary with resources information
        including hashes.

        Args:
            resources_to_check (List[Tuple]): List of resources to be checked

        Returns:
            Dict[str, Tuple]: Resources information including hashes
        """

        start_time = timer()
        results = asyncio.run(self.check_urls(resources_to_check))
        logger.info(f"Execution time: {timer() - start_time} seconds")
        return results
