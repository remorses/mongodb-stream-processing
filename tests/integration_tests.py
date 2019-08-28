import pytest
from .store_something import store_some

@pytest.mark.asyncio
def test_typical_usage():
    await asyncio.gather(
        store_some(db),
        xs,
    )