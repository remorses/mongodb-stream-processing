from funcy import collecting
from pymongo import UpdateOne
import asyncio
import operator as op
from collections.abc import AsyncIterable, AsyncIterator, Awaitable
from itertools import islice, groupby

from aiostream import operator, pipe, stream
from aiostream.core import Stream
from funcy import rcompose
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection

from mongodb_streams import accumulate_by_key, pretty, events, Batcher
from .store_something import store_some

PERSIST_INTERVAL = .4
AGGREGATED_KEY = 'likes'
ID_KEY = 'user_id'
AGGREGATED_ID_KEY = 'user_id'
EVENTS_COLLECTION = 'likes'
AGGREGATED_COLLECTION = 'aggregations'


def make_db_operation(id, value):
    update = UpdateOne(
        {AGGREGATED_ID_KEY: id, },
        {'$set': {AGGREGATED_KEY: value}, },
        upsert=True,
    )
    return update


async def main():
    db = AsyncIOMotorClient().db

    async def persist(updates, ):
        updates and await db[AGGREGATED_COLLECTION].bulk_write(updates)
        print('simulating long sleep')
        await asyncio.sleep(2)
        return 'done'
    batcher = Batcher(persist, interval=PERSIST_INTERVAL)

    def key(doc): return doc[ID_KEY]

    async def function(acc, document):
        return acc + 1

    async def initializer(doc: dict):
        value = await db[AGGREGATED_COLLECTION].find_one({AGGREGATED_ID_KEY: key(doc)})
        value = value and value.get(AGGREGATED_KEY)
        return value or 0
    # initializer = 0  # TODO rm

    xs = events(collection=db[EVENTS_COLLECTION])
    xs = accumulate_by_key(xs, function, key=key, initializer=initializer)
    xs = stream.starmap(xs, make_db_operation)
    xs = stream.map(xs, batcher.push,)  # task_limit=1)
    # xs = window(xs, PERSIST_INTERVAL)
    # xs = stream.map(xs, take_last)
    # xs = stream.map(xs, list)
    # xs = stream.map(xs, lambda x: [z[1] for z in x])
    # xs = stream.map(xs, persist, task_limit=1)
    xs = stream.map(xs, pretty, )
    await asyncio.gather(
        store_some(db),
        xs,
    )

asyncio.run(main())
