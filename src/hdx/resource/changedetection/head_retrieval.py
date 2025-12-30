"""Utility to get HTTP headers of resources. Uses asyncio."""

import asyncio
import hashlib
import logging
from io import BytesIO
from timeit import default_timer as timer
from typing import Dict, List, Set, Tuple, Optional
from urllib.parse import urlsplit

import aiohttp
from aiohttp import ClientResponseError, ClientResponse
from aiolimiter import AsyncLimiter
from openpyxl import load_workbook
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
)
from tqdm.asyncio import tqdm_asyncio

from .retrieval_utilities import check_mimetype, check_signature, zip_signature, \
    is_xlsx_file, get_http_size
from .tenacity_custom_wait import custom_wait
from .utilities import is_server_error
from .zip_crc import get_zip_crcs, match_excel_patterns

logger = logging.getLogger(__name__)


class HeadRetrieval:
    """Retrieval class for downloading and hashing resources.

    Args:
        user_agent (str): User agent string to use when downloading
        netlocs (Set[str]): Netlocs of resources to download
        xlsx_url_ignore (Optional[str]): Parts of url to ignore for special xlsx handling
    """

    def __init__(
        self,
        user_agent: str,
        netlocs: Set[str],
        xlsx_url_ignore: Optional[str] = None,
    ) -> None:
        self._user_agent = user_agent
        self._xlsx_url_ignore: Optional[str] = xlsx_url_ignore
        # Limit to 4 connections per second to a host
        self._rate_limiters = {netloc: AsyncLimiter(4, 1) for netloc in netlocs}

    async def hash_full_file(self, response: ClientResponse, url: str, resource_format: str, signature: bytes, mimetype: str) -> Tuple[str, int]:
        iterator = response.content.iter_any()
        if signature == zip_signature:
            buffer = bytearray(signature)
            async for chunk in iterator:
                buffer.extend(chunk)
            size = len(buffer)
            file_crcs = get_zip_crcs(buffer, size)
            crc_sum = 0
            if is_xlsx_file(url, resource_format, mimetype, self._xlsx_url_ignore):
                for filepath in file_crcs:
                    if match_excel_patterns(filepath):
                        crc_sum ^= file_crcs[filepath]
            else:
                for crc in file_crcs.values():
                    crc_sum ^= crc
            crc_sum = f"{crc_sum:08x}"
            return crc_sum, size

        size = len(signature)
        md5hash = hashlib.md5(signature)
        async for chunk in iterator:
            size += len(chunk)
            md5hash.update(chunk)
        return md5hash.hexdigest(), size

    @retry(
        reraise=True,
        retry=retry_if_exception(is_server_error),
        stop=stop_after_attempt(3),
        wait=custom_wait(multiplier=2, min=4),
    )
    async def fetch(
        self,
        url: str,
        resource_id: str,
        resource_format: str,
        session: aiohttp.ClientSession,
    ) -> Tuple:
        """Asynchronous code to get http headers for a resource. Returns a
        tuple with http headers including etag.

        Args:
            url (str): Resource to get
            resource_id (str): Resource id
            resource_format (str): Resource format
            session (Union[aiohttp.ClientSession, RateLimiter]): session to use for requests

        Returns:
            Tuple: Resource information including hash
        """

        async with session.get(
            url, headers={"Accept-Encoding": "identity"}, allow_redirects=True
        ) as response:
            http_status = response.status
            if http_status == 200:
                exception = ClientResponseError(
                    code=http_status,
                    message=response.reason,
                    request_info=response.request_info,
                    history=response.history,
                )
                raise exception

            headers = response.headers

            last_modified = headers.get("Last-Modified")
            final_hash = None
            etag = headers.get("Etag")
            signature = await response.content.read(4)
            sig_match = check_signature(signature, resource_format)
            mimetype = headers.get("Content-Type")
            mime_match = check_mimetype(mimetype, resource_format)
            http_size = get_http_size(headers)
            accept_ranges = headers.get("Accept-Ranges")
            # server can understand Range header
            if accept_ranges == "bytes":
                if http_size and http_size > 31457280 and signature == zip_signature:
                    return (
                        resource_id,
                        http_size,
                        last_modified,
                        final_hash,
                        sig_match,
                        mime_match,
                        http_status,
                        3  # we'll read tail, central directory and calculate crc
                    )
                # if the file is < 30Mb, it's probably cheaper to download it all than
                # make multiple requests

            if etag and signature != zip_signature:
                final_hash = etag  # we can just use the etag
                size = http_size
                status = 0
            elif http_size and http_size > 419430400:
                size = http_size
                status = 2  # too big to hash
            else:
                final_hash, size = await self.hash_full_file(response, url, resource_format, signature, mimetype)
                if not http_size or (http_size and size == http_size):
                    status = 0
                else:
                    status = 1  # size mismatch
            return (
                resource_id,
                size,
                last_modified,
                final_hash,
                sig_match,
                mime_match,
                http_status,
                status
            )

    async def process(
        self,
        metadata: Tuple,
        session: aiohttp.ClientSession,
    ) -> Tuple:
        """Asynchronous code to get http headers for a resource with rate
        limiting and exception handling. Returns a tuple with http headers
        including etag.

        Args:
            metadata (Tuple): Resource to be checked
            session (Union[aiohttp.ClientSession, RateLimiter]): session to use for requests

        Returns:
            Tuple: Header information including etag
        """
        url = metadata[0]
        resource_id = metadata[1]
        resource_format = metadata[2]

        host = urlsplit(url).netloc

        async with self._rate_limiters[host]:
            try:
                return await self.fetch(url, resource_id, resource_format, session)
            except ClientResponseError as ex:
                logger.error(f"{ex.status} {ex.message} {ex.request_info.url}")
                return resource_id, None, None, None, False, False, ex.status, -1
            except Exception as ex:
                logger.error(ex)
                return resource_id, None, None, None, False, False, -101, -2

    async def check_urls(self, resources_to_check: List[Tuple]) -> Dict[str, Tuple]:
        """Asynchronous code to get HTTP headers of resources. Return
        dictionary with resources information including etags, last modified
        and size.

        Args:
            resources_to_check (List[Tuple]): List of resources to be checked

        Returns:
            Dict[str, Tuple]: Resources information
        """
        tasks = []

        # Maximum of 10 simultaneous connections to a host
        conn = aiohttp.TCPConnector(limit_per_host=10)
        # Can set some timeouts here if needed
        timeout = aiohttp.ClientTimeout(total=5 * 60, sock_connect=30)
        async with aiohttp.ClientSession(
            connector=conn,
            timeout=timeout,
            headers={"User-Agent": self._user_agent},
        ) as session:
            for metadata in resources_to_check:
                task = self.process(metadata, session)
                tasks.append(task)
            responses = {}
            for f in tqdm_asyncio.as_completed(tasks, total=len(tasks)):
                (
                    resource_id,
                    size,
                    last_modified,
                    final_hash,
                    sig_match,
                    mime_match,
                    http_status,
                    status
                ) = await f

                responses[resource_id] = (
                    size,
                    last_modified,
                    final_hash,
                    sig_match,
                    mime_match,
                    http_status,
                    status
                )
            return responses

    def retrieve(self, resources_to_check: List[Tuple]) -> Dict[str, Tuple]:
        """Get HTTP headers of resources and hash them. Return dictionary with
        resources information including etags, last modified and size.

        Args:
            resources_to_check (List[Tuple]): List of resources to be checked

        Returns:
            Dict[str, Tuple]: Resources information including hashes
        """

        start_time = timer()
        results = asyncio.run(self.check_urls(resources_to_check))
        logger.info(f"Execution time: {timer() - start_time} seconds")
        return results
