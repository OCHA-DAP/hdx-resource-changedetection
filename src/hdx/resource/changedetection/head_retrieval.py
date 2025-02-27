"""Utility to download and hash resources. Uses asyncio. Note that the purpose of
asyncio is to help with IO-bound rather than CPU-bound code (for which multiprocessing
is more suitable as it leverages multiple CPUs). Asyncio allows you to structure your
code so that when one piece of linear single-threaded code (coroutine) is waiting for
something to happen another can take over and use the CPU. While conceptually similar to
threading, the difference is that with asyncio, it is the task of the developer rather
than the OS to decide when to switch to the next task.
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
    after_log,
    retry,
    retry_if_exception,
    wait_exponential,
)
from tqdm.asyncio import tqdm_asyncio

logger = logging.getLogger(__name__)


def is_server_error(exception):
    return isinstance(exception, aiohttp.ServerTimeoutError) or (
        isinstance(exception, aiohttp.ClientResponseError)
        and exception.status in (408, 409, 429, 500, 502, 503, 504)
    )


class HeadRetrieval:
    """Retrieval class for downloading and hashing resources.

    Args:
        user_agent (str): User agent string to use when downloading
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
        # Limit to 10 connections per second to a host
        self._rate_limiters = {
            netloc: AsyncLimiter(10, 1) for netloc in netlocs
        }

    @retry(
        reraise=True,
        retry=retry_if_exception(is_server_error),
        wait=wait_exponential(multiplier=4, min=1, max=10),
        after=after_log(logger, logging.DEBUG),
    )
    async def fetch(
        self,
        url: str,
        resource_id: str,
        session: aiohttp.ClientSession,
    ) -> Tuple:
        async with session.get(url, allow_redirects=True) as response:
            status = response.status
            if status == 200:
                headers = response.headers
                http_size = headers.get("Content-Length")
                http_last_modified = headers.get("Last-Modified")
                etag = headers.get("Etag")
                response.close()
                return resource_id, http_size, http_last_modified, etag, 200
            else:
                exception = ClientResponseError(
                    code=status,
                    message=response.reason,
                    request_info=response.request_info,
                    history=response.history,
                )
                response.close()
                raise exception

    async def process(
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

        host = urlsplit(url).netloc

        async with self._rate_limiters[host]:
            try:
                return await self.fetch(url, resource_id, session)
            except Exception as ex:
                logger.exception(ex)
                return resource_id, None, None, None, -1

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

        # Maximum of 10 simultaneous connections to a host
        conn = aiohttp.TCPConnector(limit_per_host=10)
        # Can set some timeouts here if needed
        timeout = aiohttp.ClientTimeout()
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
