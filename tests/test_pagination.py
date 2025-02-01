import pytest

from aiopynamodb.pagination import RateLimiter


class MockEventLoop:
    def __init__(self):
        self.current_time = 0.0

    def time(self):
        return self.current_time


class MockTime:
    def __init__(self):
        self.current_time = 0.0
        self._event_loop = MockEventLoop()

    async def sleep(self, amount: float) -> None:
        self.current_time += amount
        self._event_loop.current_time += amount

    def get_event_loop(self):
        return self._event_loop

    def increment_time(self, amount: float) -> None:
        self.current_time += amount
        self._event_loop.current_time += amount


def test_rate_limiter_exceptions():
    with pytest.raises(ValueError):
        r = RateLimiter(0)

    with pytest.raises(ValueError):
        r = RateLimiter(-1)

    with pytest.raises(ValueError):
        r = RateLimiter(10)
        r.rate_limit = 0

    with pytest.raises(ValueError):
        r = RateLimiter(10)
        r.rate_limit = -1


@pytest.mark.asyncio
async def test_basic_rate_limiting():
    mock_time = MockTime()
    r = RateLimiter(0.1, mock_time)  # 0.1 operations per second = 10 second delay between operations

    # 100 operations
    for i in range(0, 100):
        await r.acquire()
        # Simulates an operation that takes 1 second
        mock_time.increment_time(1)
        r.consume(1)

    # Since the first acquire doesn't take time, thus we should be expecting (100-1) * 10 seconds = 990 delay
    # plus 1 for the last increment_time(1) operation
    assert mock_time.current_time == 991.0


@pytest.mark.asyncio
async def test_basic_rate_limiting_small_increment():
    mock_time = MockTime()
    r = RateLimiter(0.1, mock_time)  # 0.1 operations per second = 10 second delay between operations

    # 100 operations
    for i in range(0, 100):
        await r.acquire()
        # Simulates an operation that takes 2 seconds
        mock_time.increment_time(2)
        r.consume(1)

    # Since the first acquire doesn't take time, thus we should be expecting (100-1) * 10 seconds = 990 delay
    # plus 2 for the last increment_time(2) operation
    assert mock_time.current_time == 992.0


@pytest.mark.asyncio
async def test_basic_rate_limiting_large_increment():
    mock_time = MockTime()
    r = RateLimiter(0.1, mock_time)  # 0.1 operations per second = 10 second delay between operations

    # 100 operations
    for i in range(0, 100):
        await r.acquire()
        # Simulates an operation that takes 11 seconds (longer than rate limit period)
        mock_time.increment_time(11)
        r.consume(1)

    # The operation takes longer than the minimum wait, so rate limiting should have no effect
    assert mock_time.current_time == 1100.0
