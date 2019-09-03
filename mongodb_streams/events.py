import bson
from funcy import collecting
from pymongo import UpdateOne
import asyncio
import operator as op
from collections.abc import AsyncIterable, AsyncIterator, Awaitable
from itertools import islice, groupby
from motor.core import Collection

from aiostream import operator, pipe, stream
from aiostream.core import Stream
from funcy import rcompose
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection

from .accumulate_by_key import accumulate_by_key
from .support import pretty
from .batcher import Batcher



@operator
async def events(collection: Collection, pipeline=[], read_token=None, store_token=None, keep_state=False, start_at_operation_time=None):
    if keep_state and not (read_token or store_token):
        raise Exception('no sense')
    if keep_state:
        iscorostore_token = asyncio.iscoroutinefunction(store_token)
        if asyncio.iscoroutinefunction(read_token):
            last_token = await read_token()
        else:
            last_token = read_token()
    else:
        last_token = None
    if not isinstance(start_at_operation_time, bson.timestamp.Timestamp):
        start_at_operation_time = bson.timestamp.Timestamp(int(start_at_operation_time), 0)

    streamer = collection.watch(
        pipeline=pipeline,
        resume_after={'_data': last_token, } if (last_token and not start_at_operation_time) else None,
        # start_at_operation_time=start_at_operation_time if start_at_operation_time else None,
        full_document='updateLookup',
    )
    async for count, change in stream.enumerate(streamer):
        if count % 5 == 0 and keep_state:
            if iscorostore_token:
                await store_token(change['_id']['_data'])
            else:
                store_token()
        yield change