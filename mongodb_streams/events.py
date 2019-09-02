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

from .accumulate_by_key import accumulate_by_key
from .support import pretty
from .batcher import Batcher



@operator
async def events(collection: AsyncIOMotorCollection, pipeline=[], read_token=None, store_token=None, keep_state=False,):
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

    streamer = collection.watch(
        pipeline=pipeline,
        resume_after={'_data': last_token, } if last_token else None,
        full_document='updateLookup',

    )
    async for count, change in stream.enumerate(streamer):
        if count % 5 == 0 and keep_state:
            if iscorostore_token:
                await store_token(change['_id']['_data'])
            else:
                store_token()
        yield change