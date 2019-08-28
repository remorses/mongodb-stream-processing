
import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Awaitable
from itertools import islice

from aiostream import pipe, stream
from aiostream.core import Stream
from funcy import rcompose

from .support import pretty, prettify

AGGREGATED_COLLECTION = ''

class Batcher:
    """
    gets items to be processed and processes them every interval,
    batch_processer must return items in the same order of batch
    every item of the batch is identified by the key function, return type must be hashable
    """
    def __init__(self, batch_processer, interval, key=str, void=True):
        self.lock = asyncio.Lock()
        self.cond = asyncio.Condition()
        self.q = asyncio.Queue()
        self.batch_processer = batch_processer
        self.interval = interval
        self.results = {}
        self.key = key
        self.void = void
        asyncio.create_task(self.consume())

    async def execute(self, batch: list):
        print(f'consuming {prettify(batch)}')
        if not self.void:
            outs = await stream.list(self.batch_processer([*batch]))
            for i, item in enumerate(batch):
                self.results[self.key(item)] = outs[i]
        else:
            await self.batch_processer([*batch])
        async with self.cond:
            self.cond.notify_all()

    async def consume(self, ):
        while True:
            try:
                await asyncio.sleep(self.interval)
            finally:
                items = []
                async with self.lock:
                    while not self.q.empty():
                        items.append(await self.q.get())
                if items:
                    await self.execute((items))

    async def push(self, elem):
        print(f'pushing {elem}')
        async with self.cond:
            async with self.lock:
                await self.q.put(elem)
            await self.cond.wait()
        if not self.void:
            return self.results[self.key(elem)]

    append = push

    # async def __anext__(self, ):
    #     while True:
    #         return await self.q.get()

async def batch_processer(batch):
    for item in batch:
        print('processed ' + str(item))
        # yield 'ciao ' + str(item)

# async def mongo_queries(batch):
#     results = await db.collection.find({
#         '$or': batch,
#     })
#     # TODO next iterations should query only the reamining results
#     for where in batch:
#         yield query(where, results)

# async def mongo_writes(updates):
#     updates and await db[AGGREGATED_COLLECTION].bulk_write(updates)

if __name__ == '__main__':

    async def main():
        b = Batcher(batch_processer, .9, void=True)
        async def delayed_push(i):
            await asyncio.sleep(.1)
            print(await b.push(i))
        x = asyncio.create_task(delayed_push('x'))
        x1 = await asyncio.gather(*[
            delayed_push(i) for i in range(3)
        ])
        x2 = asyncio.create_task(delayed_push('z'))
        x3 = await asyncio.gather(*[
            delayed_push(i) for i in range(4)
        ])

    asyncio.run(main())
