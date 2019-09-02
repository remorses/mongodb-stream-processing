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

from mongodb_streams import accumulate_by_key, pretty, events, Batcher, logger, prettify
from .store_something import store_some


PERSIST_INTERVAL = .4
ID_KEY = 'user_id'
AGGREGATED_KEY = 'likes_windows'
AGGREGATED_ID_KEY = 'aggregated_user_id'
EVENTS_COLLECTION = 'likes'
AGGREGATED_COLLECTION = 'windows_aggregations'


def make_db_operation(id, window):
        filter = {
            AGGREGATED_ID_KEY: id,
        }
        # if await target_collection.find_one({**filter, f"{target_result_key}.start": window['start']}): 
        # TODO should check also on other ongoing update requests
        # or should find a way to upsert the first operation
        return [
            UpdateOne(
                {**filter, f"{AGGREGATED_KEY}.start": window['start']},
                {'$set': {f"{AGGREGATED_KEY}.$.value": window['value']}},
                upsert=False,
            ),
            UpdateOne(
                {**filter, f"{AGGREGATED_KEY}.start": {'$ne' : window['start']}},
                {'$push': {AGGREGATED_KEY: window}},
                upsert=False,
            ),
        ]

async def get_old_windows(id, from_timestamp: int): # based last event timestamp
        target = await db[AGGREGATED_COLLECTION].find_one({
            AGGREGATED_ID_KEY : id,
        })
        target = target or {}
        if AGGREGATED_KEY in target:
            old_windows = [w for w in target[AGGREGATED_KEY] if w['start'] > from_timestamp] # timestamp - INTERVAL * GRID_SIZE
            old_windows = sorted(old_windows, key=lambda x: x['start'])
            # old_windows = reversed(old_windows)
        else:
            old_windows = [] # make_windows([], datetime.utcnow(), default)

        logger.info('got old windows ' + prettify(old_windows))
        return old_windows


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


    # initializer = 0  # TODO rm

    xs = events(collection=db[EVENTS_COLLECTION])
    xs = accumulate_by_key(xs, function, key=key, initializer=get_old_windows)
    xs = stream.starmap(xs, make_db_operation)
    xs = stream.concat(xs, )
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
