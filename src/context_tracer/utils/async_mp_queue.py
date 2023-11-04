import asyncio
import concurrent.futures
import multiprocessing as mp
import multiprocessing.queues as mpq
from functools import partial
from multiprocessing.context import BaseContext
from typing import Any


class AsyncMultiProcessQueue(mpq.Queue):
    """
    Multiprocessing queue that can be used in an async context.

    Uses a thread pool to run blocking operations in a non-blocking way.
    For example, this is useful to use in a FastAPI server, where the main thread is blocked by the server, but you want to use a queue to communicate with other processes.

    For more complex use cases, consider using a library/service like RabbitMQ or Redis.
    Doesn't work with ProcessPoolExecutor.

    More info:
    - https://docs.python.org/3/library/multiprocessing.html#pipes-and-queues
    - https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.run_in_executor
    - https://stackoverflow.com/questions/73560520/how-to-use-queue-correctly-with-processpoolexecutor-in-python
    - https://stackoverflow.com/questions/24687061/can-i-somehow-share-an-asynchronous-queue-with-a-subprocess
    """

    def __init__(self, maxsize=0, *, ctx: BaseContext | None = None):
        self.__executor = concurrent.futures.ThreadPoolExecutor()
        if ctx is None:
            ctx = mp.get_context()
        super().__init__(maxsize, ctx=ctx)

    @property
    def _executor(self):
        if self.__executor is None:
            self.__executor = concurrent.futures.ThreadPoolExecutor()
        return self.__executor

    async def get_async(self, block=True, timeout=None) -> Any:
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, partial(super().get, block=block, timeout=timeout)
        )

    async def put_async(self, obj, block=True, timeout=None) -> None:
        await asyncio.get_running_loop().run_in_executor(
            self._executor, partial(super().put, block=block, timeout=timeout), obj
        )
