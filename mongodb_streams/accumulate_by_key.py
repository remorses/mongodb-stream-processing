import operator as op
from collections import defaultdict
import asyncio
from itertools import islice
from funcy import rcompose
from aiostream import stream, pipe, operator, streamcontext
from aiostream.core import Stream
from aiostream.aiter_utils import anext


@operator(pipable=True)
async def accumulate_by_key(source: Stream, func=op.add, key=lambda x: None, initializer=lambda x: 0):
    """Generate a series of accumulated sums (or other binary function)
    from an asynchronous sequence.

    If ``initializer`` is present, it is placed before the items
    of the sequence in the calculation, and serves as a default
    when the sequence is empty.
    """
    accumulated = {}
    if not callable(initializer):
        _init = initializer
        initializer = lambda x: _init
    
    iscorofunc = asyncio.iscoroutinefunction(func)
    iscoroinitializer = asyncio.iscoroutinefunction(initializer)
    async with streamcontext(source) as streamer:
        async for item in streamer:
            id = key(item)
            if id in accumulated:
                past = accumulated[id]
            else:
                past = initializer(item)
                if iscoroinitializer:
                    print('initializing')
                    past = await past
                    print('got ' + str(past))
            value = func(past, item)
            if iscorofunc:
                value = await value
            accumulated[id] = value
            yield id, value