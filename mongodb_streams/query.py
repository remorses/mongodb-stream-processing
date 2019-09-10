import asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCursor, AsyncIOMotorCollection


async def find_one(collection: AsyncIOMotorCollection, match, pipeline=[]):
    cursor: AsyncIOMotorCursor = collection.aggregate([
        {'$match': match},
        {'$limit': 1},
        *pipeline,
    ])
    if await cursor.fetch_next:
        return cursor.next_object()
    else:
        return None


async def find(collection: AsyncIOMotorCollection, match={}, pipeline=[], sort=None, limit=20, skip=0, max_len=20):
    pipe: list = []
    match and pipe.append({'$match': match})
    sort and pipe.append({'$sort': sort})
    limit and pipe.append({"$limit": skip + limit})
    skip and pipe.append({"$skip": skip})
    pipeline = pipe + pipeline
    cursor: AsyncIOMotorCursor = collection.aggregate(pipeline)
    return await cursor.to_list(max_len)

async def count_documents(collection, match, pipeline=[]):
    cursor: AsyncIOMotorCursor = collection.aggregate([
        {'$match': match},
        *pipeline,
        {'$count': 'count',},
    ])
    if await cursor.fetch_next:
        object = cursor.next_object()
        return object['count']


