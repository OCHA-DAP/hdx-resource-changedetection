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
    is_filestore_host,
    is_xlsx_file,
    zip_signature,
)
from .tenacity_custom_wait import custom_wait
from .utilities import is_server_error

logger = logging.getLogger(__name__)

# Set maximum allowed download size for full streams (1 GB)
MAX_DOWNLOAD_SIZE = 1073741824

# Set file size threshold above which etag is used rather than hashing regardless of
# file type
ETAG_SIZE_THRESHOLD = 419430400

# Set file size threshold above which crc is performed rather than hashing
CRC_SIZE_THRESHOLD = 31457280

# Set file size threshold above which we stream and hash xlsx and zips rather than
# trying to hold in memory
ZIP_SIZE_THRESHOLD = 78643200

# Set limit per host for filestore resources
FILESTORE_LIMIT_PER_HOST = 10

# Set limit per host for all other resources
DEFAULT_LIMIT_PER_HOST = 4


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

        # Apply the limit based on the host
        self._rate_limiters = {}
        for netloc in netlocs:
            if is_filestore_host(netloc):
                self._rate_limiters[netloc] = AsyncLimiter(FILESTORE_LIMIT_PER_HOST, 1)
            else:
                self._rate_limiters[netloc] = AsyncLimiter(DEFAULT_LIMIT_PER_HOST, 1)

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
            if response.status != 206:
                logger.warning(
                    f"Server ignored Range request for tail on {url} (Status: {response.status})"
                )
                return ""
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
            size = len(signature)

            # Read into the buffer, but strictly monitor the size
            async for chunk in iterator:
                newsize = size + len(chunk)

                if newsize > ZIP_SIZE_THRESHOLD:
                    # --- TRANSITION TO STREAMING ---
                    md5_stream = hashlib.md5(buffer)
                    md5_stream.update(chunk)  # Don't lose the current chunk!
                    size = newsize
                    del buffer  # Free memory immediately

                    # Continue streaming the remainder of the file
                    async for remaining_chunk in iterator:
                        size += len(remaining_chunk)
                        md5_stream.update(remaining_chunk)

                    return md5_stream.hexdigest(), 1, size

                # If still under limit, keep buffering
                size = newsize
                buffer.extend(chunk)

            # If the loop finishes naturally, the file fit in memory
            if is_xlsx:
                xlhash = hash_excel_buffer(buffer)
                if xlhash:
                    del buffer
                    return xlhash, 2, size
            else:
                crc_sum = crc_zip_buffer(buffer)
                if crc_sum:
                    del buffer
                    return crc_sum, 3, size

            md5hash = hashlib.md5(buffer).hexdigest()  # fallback
            del buffer
            return md5hash, 4, size

        # Non-zip files naturally stream safely chunk by chunk without a heavy buffer
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
        resource_id: str,
        url: str,
        resource_format: str,
        existing_hash: str,
        session: ClientSession,
    ) -> tuple:
        """Asynchronous code to get http headers for a resource. Returns a
        tuple with http headers including etag.

        Args:
            resource_id (str): Resource id
            url (str): Resource to get
            resource_format (str): Resource format
            existing_hash (str): Existing hash
            session (Union[ClientSession, RateLimiter]): session to use for requests

        Returns:
            Tuple: Resource information including hash
        """
        try_crc = False

        # ==========================================
        # STEP 1: INITIAL PROBE & STREAM
        # ==========================================
        async with session.get(
            url, headers={"Accept-Encoding": "identity"}, allow_redirects=True
        ) as response:
            http_status = response.status
            if http_status != 200:
                raise ClientResponseError(
                    status=http_status,
                    message=response.reason,
                    request_info=response.request_info,
                    history=response.history,
                )

            headers = response.headers
            last_modified = headers.get("Last-Modified")
            etag = headers.get("Etag")
            mimetype = headers.get("Content-Type")
            http_size = get_http_size(headers)
            accept_ranges = headers.get("Accept-Ranges")

            signature = await response.content.read(4)

            sig_match = check_signature(signature, resource_format)
            mime_match = check_mimetype(mimetype, resource_format)
            is_xlsx = is_xlsx_file(
                url, resource_format, mimetype, self._xlsx_url_ignore
            )
            size_match = None
            # 1: The etag equals the existing hash (if it's in an etag)
            if etag and existing_hash and etag == existing_hash:
                return (
                    resource_id,
                    http_size,
                    last_modified,
                    existing_hash,
                    existing_hash,
                    sig_match,
                    mime_match,
                    size_match,
                    http_status,
                    6,
                )

            # 2: Does it need a CRC check?
            if (
                accept_ranges == "bytes"
                and http_size
                and http_size > CRC_SIZE_THRESHOLD
                and signature == zip_signature
                and not is_xlsx
            ):
                # Flag it and naturally exit the async with block to free the connection pool!
                try_crc = True

                # If NO CRC is needed, process immediately using this already-open connection!
            else:
                # 3: ETag Fast-Paths
                if http_size and http_size > ETAG_SIZE_THRESHOLD and etag:
                    return (
                        resource_id,
                        http_size,
                        last_modified,
                        etag,
                        etag,
                        sig_match,
                        mime_match,
                        size_match,
                        http_status,
                        6,
                    )

                if etag and signature != zip_signature:
                    return (
                        resource_id,
                        http_size,
                        last_modified,
                        etag,
                        etag,
                        sig_match,
                        mime_match,
                        size_match,
                        http_status,
                        5,
                    )

                # 4: Hard cap for massive files without ETags
                if http_size and http_size > MAX_DOWNLOAD_SIZE:
                    return (
                        resource_id,
                        http_size,
                        last_modified,
                        etag,
                        None,
                        sig_match,
                        mime_match,
                        size_match,
                        http_status,
                        -1,
                    )

                # 5: Full File Hash (Streaming on the ORIGINAL connection)
                final_hash, status, size = await self.hash_full_file(
                    response, signature, is_xlsx
                )
                if http_size:
                    size_match = bool(http_size == size)
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

        # ==========================================
        # STEP 2: DEFERRED CRC & FALLBACKS
        # (We only reach here if try_crc == True. The original connection is safely closed).
        # ==========================================
        if try_crc:
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

            # --- YOUR FALLBACK LOGIC RESTORED ---

            # Fallback 1: ETag if massive
            if http_size and http_size > ETAG_SIZE_THRESHOLD and etag:
                return (
                    resource_id,
                    http_size,
                    last_modified,
                    etag,
                    etag,
                    sig_match,
                    mime_match,
                    size_match,
                    http_status,
                    6,
                )

            # Fallback 2: Hard cap for massive files without ETags
            if http_size and http_size > MAX_DOWNLOAD_SIZE:
                return (
                    resource_id,
                    http_size,
                    last_modified,
                    etag,
                    None,
                    sig_match,
                    mime_match,
                    size_match,
                    http_status,
                    -1,
                )

            # Fallback 3: Full Hash. Must open a new connection since the first one was closed.
            async with session.get(
                url, headers={"Accept-Encoding": "identity"}, allow_redirects=True
            ) as fallback_response:
                if fallback_response.status != 200:
                    raise ClientResponseError(
                        status=fallback_response.status,
                        message=fallback_response.reason,
                        request_info=fallback_response.request_info,
                        history=fallback_response.history,
                    )

                fallback_signature = await fallback_response.content.read(4)
                final_hash, status, size = await self.hash_full_file(
                    fallback_response, fallback_signature, is_xlsx
                )

                if http_size:
                    size_match = bool(http_size == size)

                return (
                    resource_id,
                    size,
                    last_modified,
                    etag,
                    final_hash,
                    sig_match,
                    mime_match,
                    size_match,
                    fallback_response.status,
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
        resource_id = metadata[1]
        url = metadata[2]
        resource_format = metadata[3]
        existing_hash = metadata[4]
        host = urlsplit(url).netloc

        # Add a fallback limiter for unknown/redirected hosts
        if host not in self._rate_limiters:
            # If it's HDX, allow FILESTORE_LIMIT_PER_HOST requests per second.
            # Otherwise, DEFAULT_LIMIT_PER_HOST requests per second.
            if is_filestore_host(host):
                self._rate_limiters[host] = AsyncLimiter(FILESTORE_LIMIT_PER_HOST, 1)
            else:
                self._rate_limiters[host] = AsyncLimiter(DEFAULT_LIMIT_PER_HOST, 1)
        async with self._rate_limiters[host]:
            try:
                return await self.fetch(
                    resource_id, url, resource_format, existing_hash, session
                )
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
                logger.error(f"Error processing {resource_id}: {repr(ex)}")
                return (
                    resource_id,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    -101,
                    -11,
                )

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
        # ==========================================
        # 1. EVENT LOOP LIMITER
        # ==========================================
        # Keeps the Jenkins CPU and base memory footprint healthy by preventing
        # thousands of tasks from being scheduled on the event loop simultaneously.
        task_semaphore = asyncio.Semaphore(500)

        # ==========================================
        # CUSTOM PER-HOST CONCURRENCY
        # ==========================================
        host_semaphores = {}

        # Safely pre-populate the semaphores synchronously to avoid async race conditions
        for metadata in resources_to_check:
            host = urlsplit(metadata[2]).netloc
            if host not in host_semaphores:
                if is_filestore_host(host):
                    host_semaphores[host] = asyncio.Semaphore(FILESTORE_LIMIT_PER_HOST)
                else:
                    host_semaphores[host] = asyncio.Semaphore(DEFAULT_LIMIT_PER_HOST)

        async def sem_process(metadata, session):
            url = metadata[2]
            host = urlsplit(url).netloc

            async with task_semaphore:
                # Enforce the specific limit for this host
                async with host_semaphores[host]:
                    return await self.process(metadata, session)

        # ==========================================
        # 2. GLOBAL CONNECTION LIMITER
        # ==========================================
        # limit restricts the absolute total number of active downloads across ALL hosts.
        conn = TCPConnector(limit=13)

        # Can set some timeouts here if needed
        timeout = ClientTimeout(
            total=30 * 60,  # Absolute ceiling: 30 minutes max per task
            connect=None,  # Allow waiting in TCPConnector queue as long as needed
            sock_connect=30,  # After leaving queue, fail if TCP handshake takes >30s
            sock_read=30,  # During download, fail if server stops sending data for >30s
        )

        async with ClientSession(
            connector=conn,
            timeout=timeout,
            headers={"User-Agent": self._user_agent},
        ) as session:
            # Queue up the tasks using the new semaphored wrapper
            for metadata in resources_to_check:
                task = asyncio.create_task(sem_process(metadata, session))
                tasks.append(task)

            responses = {}
            # Process them as they complete
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
