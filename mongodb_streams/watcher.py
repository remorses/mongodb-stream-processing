import typing
import asyncio
from itertools import islice
from funcy import rcompose
from aiostream import stream, pipe
from aiostream.core import Stream
from collections.abc import AsyncIterable, Awaitable, AsyncIterator


class Watcher(AsyncIterator):
    def __init__(self):
        self.stopped = False
        self.lock = asyncio.Lock()
        self.futures: typing.List[asyncio.Future] = []
        asyncio.create_task(self.consume())

    async def __anext__(self, ):
        if self.stopped:
            raise StopAsyncIteration()
        future = asyncio.Future()
        async with self.lock:
            self.futures.append(future)
        result = await future
        if isinstance(result, StopAsyncIteration):
            raise result
        return result

    async def generator(self,):
        raise NotImplementedError()

    async def consume(self, ):
        try:
            async for elem in self.generator():
                async with self.lock:
                    for f in self.futures:
                        f.set_result(elem)
                    self.futures = []
        finally:
            # print('finally')
            self.stopped = True
            async with self.lock:
                for f in self.futures:
                    f.set_result(StopAsyncIteration())
                self.futures = []
