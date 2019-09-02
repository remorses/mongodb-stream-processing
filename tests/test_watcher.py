import asyncio
import pytest
from mongodb_streams.watcher import Watcher

@pytest.mark.asyncio
async def test_basic():
    
    async def generator(n):
        for i in range(n):
            print(f'generating {i}')
            yield i
            await asyncio.sleep(.2)
        return
    watcher = Watcher(generator(5))
    async def consumer(n):
        async for i in watcher:
            print(f'{n} consumed {i}')
        print(f'ended {n}')
    await asyncio.sleep(.3)
    await asyncio.gather(
        consumer(1),
        consumer(2),
    )
    async for i in watcher:
        print(f'{i} after end')

if __name__ == '__main__':
    asyncio.run(test_basic())