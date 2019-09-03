from funcy import collecting
from pymongo import UpdateOne
import asyncio
import operator as op
from collections.abc import AsyncIterable, AsyncIterator, Awaitable
from itertools import islice, groupby
from mongomock.aggregate import process_pipeline

from aiostream import operator, pipe, stream
from aiostream.core import Stream
from funcy import rcompose
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection

from mongodb_streams import accumulate_by_key, prettify, pretty, events, Batcher, Watcher, window, find, find_one
from .store_something import store_some

PERSIST_INTERVAL = .4
AGGREGATED_KEY = 'likes'
ID_KEY = 'user_id'
AGGREGATED_ID_KEY = 'user_id'
EVENTS_COLLECTION = 'likes'
AGGREGATED_COLLECTION = 'aggregations'

BATCH_INTERVAL = 1

def take_last(array):
    array = list(array)
    return array[-1] if array else None

def last_per_window(xs):
    xs = stream.map(xs, take_last)
    xs = stream.filter(xs, bool)
    return xs

def aggregate(array, pipeline) -> list:
    return list(process_pipeline(array, None, pipeline, None))



def get_where(pipeline):
    pipeline = [x for x in pipeline if '$match' in x.keys()]
    if pipeline:
        return pipeline[0]['$match']
    else:
        return {}

async def single_resolver(collection, watcher, pipeline=[]):
    initializer = await find_one(collection, get_where(pipeline) or {})
    yield initializer
    xs = stream.filter(watcher, lambda change: change['operationType'] == 'update',)
    xs = stream.map(xs, lambda change: change['fullDocument'], task_limit=1)
    xs = stream.filter(xs, lambda doc: doc['_id'] == initializer['_id'])
    xs = stream.concatmap(
        xs, 
        lambda w: stream.iterate(aggregate([w], pipeline)), 
        task_limit=1
    )
    xs = window(xs, BATCH_INTERVAL)
    xs = last_per_window(xs, )
    # xs = last_per_window(xs, )
    # xs = stream.map(xs, list)
    # xs = stream.concat(xs, )
    async for x in xs:
        print(f'serving {prettify(x)}')
        yield x

async def multi_resolver(collection: AsyncIOMotorCollection, watcher, pipeline=[]):
    documents: list = await find(collection, match=get_where(pipeline), )
    documents = documents[:3]
    documents: dict = {doc['_id']: aggregate([doc], pipeline)[0] for doc in documents}
    yield list(documents.values())


    def process(change):
        document = change['fullDocument']
        _id = document['_id']
        if change['operationType'] == 'insert':
            documents.update({_id: aggregate([document], pipeline)[0]})
            return list(documents.values())
        elif change['operationType'] == 'update':
            if _id in documents.keys():
                documents[_id] = aggregate([document], pipeline)[0]
                return list(documents.values())

    xs = stream.map(watcher, process, task_limit=1)
    xs = stream.filter(xs, bool)
    xs = window(xs, BATCH_INTERVAL)
    xs = last_per_window(xs, )
    async for x in xs:
        yield list(x)

async def main():
    db = AsyncIOMotorClient().db
    collection = db[EVENTS_COLLECTION]
    xs = events(collection, )
    watcher = Watcher(xs)
    # xs = stream.map(watcher, pretty, )
    async def consume():
        streamer = multi_resolver(collection, watcher, pipeline=[]) # {'$match': {'user_id': 1}}
        async for x in streamer:
            print(f'got {prettify(x)}')
            # print(x)

    asyncio.create_task(consume())
    # xs = accumulate_by_key(xs, function, key=key, initializer=initializer)
    # xs = stream.starmap(xs, make_db_operation)
    # xs = stream.map(xs, batcher.push,)  # task_limit=1)
    # xs = window(xs, PERSIST_INTERVAL)
    # xs = stream.map(xs, take_last)
    # xs = stream.map(xs, list)
    # xs = stream.map(xs, lambda x: [z[1] for z in x])
    # xs = stream.map(xs, persist, task_limit=1)
    
    await asyncio.gather(
        store_some(db),
        xs,
    )

asyncio.run(main())
