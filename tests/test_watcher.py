import asyncio
import pytest
from mongodb_streams.watcher import Watcher

@pytest.mark.asyncio
async def test_basic():
    class RangeWatcher(Watcher):
        def __init__(self, n):
            self.n = n
            super().__init__()

        async def generator(self, ):
            for i in range(self.n):
                print(f'generating {i}')
                yield i
                await asyncio.sleep(.2)
            return

    watcher = RangeWatcher(6)
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