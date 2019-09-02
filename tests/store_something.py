
import asyncio
import time
import pymongo
from motor.motor_asyncio import AsyncIOMotorClient


async def store_some(db: AsyncIOMotorClient):
    await asyncio.sleep(.6)
    # loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
    # for i in range(20):
    #     # print(f'storing {i}')
    #     await db['likes'].insert_one({'user_id': i})
    n = 7
    for i in range(n * 5):
        await db['likes'].update_one({'user_id': i % n}, {'$set': {'ciao': 'xxx' + str(i)}})
        await asyncio.sleep(.2)
        


if __name__ == '__main__':
    asyncio.run(store_some(AsyncIOMotorClient().db))