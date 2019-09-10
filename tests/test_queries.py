import pytest
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from motor.core import Database, Collection
from mongodb_streams import count_documents, find

@pytest.fixture
async def collection() -> Collection:
    db: Database = AsyncIOMotorClient().db_no_one_uses
    await db.drop_collection('test_collection')
    yield db.test_collection


@pytest.mark.asyncio
async def test_count_documents(collection: Collection):
    N = 7
    await collection.insert_many([{str(i): i} for i in range(N)])
    count = await count_documents(collection, {})
    print(count)
    assert count == N

@pytest.mark.asyncio
async def test_count_documents_with_pipeline(collection: Collection):
    N = 7
    pipeline = [
        {'$group': {
            '_id': None,
            'sum': {'$sum': '$value'}
        }}
    ]
    await collection.insert_many([{'value': i} for i in range(N)])
    count = await count_documents(collection, {}, pipeline=pipeline)
    docs = await find(collection, {}, pipeline=pipeline)
    assert count == len(docs)