"""Utility to get HTTP headers of resources. Uses asyncio.
"""

import asyncio
import logging
from timeit import default_timer as timer
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlsplit

import aiohttp
from aiohttp import ClientResponseError
from aiolimiter import AsyncLimiter
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
)
from tqdm.asyncio import tqdm_asyncio

from .tenacity_custom_wait import custom_wait
from .utilities import is_server_error

logger = logging.getLogger(__name__)


class HeadRetrieval:
    """Retrieval class for downloading and hashing resources.

    Args:
        user_agent (str): User agent string to use when downloading
        netlocs (Set[str]): Netlocs of resources to download
        url_ignore (Optional[str]): Parts of url to ignore for special xlsx handling
    """

    def __init__(
        self,
        user_agent: str,
        netlocs: Set[str],
        url_ignore: Optional[str] = None,
    ) -> None:
        self._user_agent = user_agent
        self._url_ignore: Optional[str] = url_ignore
        # Limit to 4 connections per second to a host
        self._rate_limiters = {
            netloc: AsyncLimiter(4, 1) for netloc in netlocs
        }

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
        session: aiohttp.ClientSession,
    ) -> Tuple:
        """Asynchronous code to get http headers for a resource. Returns a
        tuple with http headers including etag.

        Args:
            url (str): Resource to get
            resource_id (str): Resource id
            session (Union[aiohttp.ClientSession, RateLimiter]): session to use for requests

        Returns:
            Tuple: Resource information including hash
        """
        async with session.head(url, allow_redirects=True) as response:
            status = response.status
            if status == 200:
                headers = response.headers
                size = headers.get("Content-Length")
                if size:
                    size = int(size)
                last_modified = headers.get("Last-Modified")
                etag = headers.get("Etag")
                return resource_id, size, last_modified, etag, 200
            else:
                exception = ClientResponseError(
                    code=status,
                    message=response.reason,
                    request_info=response.request_info,
                    history=response.history,
                )
                raise exception

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

        host = urlsplit(url).netloc

        async with self._rate_limiters[host]:
            try:
                return await self.fetch(url, resource_id, session)
            except ClientResponseError as ex:
                logger.error(f"{ex.status} {ex.message} {ex.request_info.url}")
                return resource_id, None, None, None, ex.status
            except Exception as ex:
                logger.error(ex)
                return resource_id, None, None, None, -101

    async def check_urls(
        self, resources_to_check: List[Tuple]
    ) -> Dict[str, Tuple]:
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
                    http_size,
                    http_last_modified,
                    etag,
                    status,
                ) = await f

                responses[resource_id] = (
                    http_size,
                    http_last_modified,
                    etag,
                    status,
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
