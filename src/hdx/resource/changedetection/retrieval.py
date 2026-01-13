"""Utility to download and hash resources. Uses asyncio."""

import asyncio
import hashlib
import logging
from timeit import default_timer as timer
from urllib.parse import urlsplit

from aiohttp import (
    ClientResponse,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
    TCPConnector,
)
from aiolimiter import AsyncLimiter
from hdx.utilities.file_hashing import (
    crc_zip_buffer,
    hash_excel_buffer,
)
from hdx.utilities.zip_crc import (
    get_crc_sum,
    get_zip_cd_header,
    get_zip_tail_header,
    parse_central_directory,
)
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
)
from tqdm.asyncio import tqdm_asyncio

from .retrieval_utilities import (
    check_mimetype,
    check_signature,
    get_http_size,
    is_xlsx_file,
    zip_signature,
)
from .tenacity_custom_wait import custom_wait
from .utilities import is_server_error

logger = logging.getLogger(__name__)


class Retrieval:
    """Retrieval class for downloading and hashing resources.

    Args:
        user_agent (str): User agent string to use when downloading
        netlocs (Set[str]): Netlocs of resources to download
        xlsx_url_ignore (Optional[str]): Parts of url to ignore for special xlsx handling
    """

    def __init__(
        self,
        user_agent: str,
        netlocs: set[str],
        xlsx_url_ignore: str | None = None,
    ) -> None:
        self._user_agent = user_agent
        self._xlsx_url_ignore: str | None = xlsx_url_ignore
        # Limit to 4 connections per second to a host
        self._rate_limiters = {netloc: AsyncLimiter(4, 1) for netloc in netlocs}

    @retry(
        reraise=True,
        retry=retry_if_exception(is_server_error),
        stop=stop_after_attempt(3),
        wait=custom_wait(multiplier=2, min=4),
    )
    async def get_async_crc_sum(
        self, session: ClientSession, url: str, size: int
    ) -> str:
        header_tail = get_zip_tail_header(size)

        async with session.get(
            url, headers=header_tail, allow_redirects=True
        ) as response:
            tail_data = await response.read()

        total_records, headers_cd = get_zip_cd_header(tail_data)
        if total_records == -1:
            return ""
        async with session.get(
            url, headers=headers_cd, allow_redirects=True
        ) as response:
            cd_data = await response.read()
        file_crcs = parse_central_directory(cd_data, total_records)
        if not file_crcs:
            return ""
        return get_crc_sum(file_crcs)

    @staticmethod
    async def hash_full_file(
        response: ClientResponse, signature: bytes, is_xlsx: bool
    ) -> tuple[str, int, int]:
        iterator = response.content.iter_any()
        if signature == zip_signature:
            buffer = bytearray(signature)
            async for chunk in iterator:
                buffer.extend(chunk)
            size = len(buffer)
            if is_xlsx:
                md5hash = hash_excel_buffer(buffer)
                if md5hash:
                    del buffer
                    return md5hash, 2, size
            else:
                crc_sum = crc_zip_buffer(buffer)
                if crc_sum:
                    del buffer
                    return crc_sum, 3, size
            md5hash = hashlib.md5(buffer).hexdigest()  # fallback
            del buffer
            return md5hash, 4, size

        size = len(signature)
        md5hash = hashlib.md5(signature)
        async for chunk in iterator:
            size += len(chunk)
            md5hash.update(chunk)
        return md5hash.hexdigest(), 1, size

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
        session: ClientSession,
    ) -> tuple:
        """Asynchronous code to get http headers for a resource. Returns a
        tuple with http headers including etag.

        Args:
            url (str): Resource to get
            resource_id (str): Resource id
            resource_format (str): Resource format
            session (Union[ClientSession, RateLimiter]): session to use for requests

        Returns:
            Tuple: Resource information including hash
        """

        async with session.get(
            url, headers={"Accept-Encoding": "identity"}, allow_redirects=True
        ) as response:
            http_status = response.status
            if http_status != 200:
                exception = ClientResponseError(
                    code=http_status,
                    message=response.reason,
                    request_info=response.request_info,
                    history=response.history,
                )
                raise exception

            headers = response.headers

            last_modified = headers.get("Last-Modified")
            etag = headers.get("Etag")
            signature = await response.content.read(4)
            sig_match = check_signature(signature, resource_format)
            mimetype = headers.get("Content-Type")
            mime_match = check_mimetype(mimetype, resource_format)
            http_size = get_http_size(headers)
            size_match = None
            accept_ranges = headers.get("Accept-Ranges")
            is_xlsx = is_xlsx_file(
                url, resource_format, mimetype, self._xlsx_url_ignore
            )

            # server can understand Range header
            if accept_ranges == "bytes":
                if (
                    http_size
                    and http_size > 31457280
                    and signature == zip_signature
                    and not is_xlsx
                ):
                    response.close()
                    final_hash = await self.get_async_crc_sum(session, url, http_size)
                    if final_hash:
                        return (
                            resource_id,
                            http_size,
                            last_modified,
                            etag,
                            final_hash,
                            sig_match,
                            mime_match,
                            size_match,
                            http_status,
                            7,
                        )
                # if the file is < 30Mb, it's probably cheaper to download it all than
                # make multiple requests

            if http_size and http_size > 419430400:
                size = http_size
                if etag:
                    final_hash = etag  # we use etag because file is too big to hash
                    status = 6
                else:
                    final_hash = None
                    status = -1  # too big to hash
            elif etag and signature != zip_signature:
                final_hash = etag  # we can just use the etag
                size = http_size
                status = 5
            else:
                # returns a status of 1, 2, 3 or 4
                final_hash, status, size = await self.hash_full_file(
                    response, signature, is_xlsx
                )
                if http_size:
                    if http_size == size:
                        size_match = True
                    else:
                        size_match = False  # size mismatch
            return (
                resource_id,
                size,
                last_modified,
                etag,
                final_hash,
                sig_match,
                mime_match,
                size_match,
                http_status,
                status,
            )

    async def process(
        self,
        metadata: tuple,
        session: ClientSession,
    ) -> tuple:
        """Asynchronous code to get http headers for a resource with rate
        limiting and exception handling. Returns a tuple with http headers
        including etag.

        Args:
            metadata (Tuple): Resource to be checked
            session (Union[ClientSession, RateLimiter]): session to use for requests

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
                return (
                    resource_id,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    ex.status,
                    -10,
                )
            except Exception as ex:
                logger.error(ex)
                return resource_id, None, None, None, None, None, None, None, -101, -11

    async def check_urls(self, resources_to_check: list[tuple]) -> dict[str, tuple]:
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
        conn = TCPConnector(limit_per_host=10)
        # Can set some timeouts here if needed
        timeout = ClientTimeout(total=5 * 60, sock_connect=30)
        async with ClientSession(
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
                    etag,
                    final_hash,
                    sig_match,
                    mime_match,
                    size_match,
                    http_status,
                    status,
                ) = await f

                responses[resource_id] = (
                    size,
                    last_modified,
                    etag,
                    final_hash,
                    sig_match,
                    mime_match,
                    size_match,
                    http_status,
                    status,
                )
            return responses

    def retrieve(self, resources_to_check: list[tuple]) -> dict[str, tuple]:
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
