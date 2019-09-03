import asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCursor, AsyncIOMotorCollection
from motor.core import Collection
from aiostream import stream
from datetime import datetime
import json
from mongodb_streams import events, prettify

async def tester(db):
    coll: Collection = db.collection
    for i in range(3):
        await coll.delete_many({'xxx': i})
        await coll.insert_one({'xxx': i})
    # await coll.delete_many({'xxx': 0})
    # await coll.insert_one({'xxx': 4})
    await asyncio.sleep(.2)
    await coll.update_many({'xxx': 0}, {'$set': {'yyy': 0}},)


async def main():
    now = datetime.utcnow().timestamp()
    db = AsyncIOMotorClient().db
    streamer = events(db.collection, start_at_operation_time=now - 60)
    asyncio.create_task(tester(db))
    async for i, x in stream.enumerate(streamer):
        print(f'[{i}]', prettify(x['fullDocument'] if 'fullDocument' in x else x))


asyncio.run(main())
