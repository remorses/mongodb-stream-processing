from pymongo import UpdateOne
import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Awaitable
from itertools import islice

from aiostream import operator, pipe, stream, streamcontext
from aiostream.core import Stream
from funcy import rcompose
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection


def milliseconds(secs): return int(secs * 1000)


def round_time_to(interval, timestamp, ):
    t = int(timestamp)
    return int(t - (t % interval))


@operator(pipable=True)
async def window(source, interval):
    """Make sure the elements of an asynchronous sequence are separated
    in time by the given interval.
    """
    def roundend_loop_time(loop): return round_time_to(
        interval, milliseconds(loop.time()))
    interval = milliseconds(interval)
    loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
    items = []
    time = last_time = roundend_loop_time(loop)
    async with streamcontext(source) as streamer:
        async for item in streamer:
            time = roundend_loop_time(loop)
            if time == last_time:
                items.append(item)
            else:
                yield items + [item]
                items = []
            last_time = time
        yield items
