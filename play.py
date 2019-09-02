import asyncio

async def main():
    f = asyncio.Future()
    async def waiter(f):
        print(await f + ' done')
        print('end')
    asyncio.create_task(waiter(f))
    f.cancel()
    

asyncio.run(main())