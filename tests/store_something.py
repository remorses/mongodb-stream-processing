
import asyncio
import time
import pymongo
from motor.motor_asyncio import AsyncIOMotorClient


async def store_some(db: AsyncIOMotorClient):
    # loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
    for i in range(20):
        # print(f'storing {i}')
        await db['likes'].insert_one({'user_id': i % 6})
        await asyncio.sleep(.2)


if __name__ == '__main__':
    asyncio.run(store_some(AsyncIOMotorClient().db))