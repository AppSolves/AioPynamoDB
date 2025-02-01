import asyncio
from typing import Any, Callable, Dict, Iterable, AsyncIterator, Optional, TypeVar

from aiopynamodb.constants import (CAMEL_COUNT, ITEMS, LAST_EVALUATED_KEY, SCANNED_COUNT,
                                CONSUMED_CAPACITY, TOTAL, CAPACITY_UNITS)

_T = TypeVar('_T')


class RateLimiter:
    """
    RateLimiter limits operations to a pre-set rate of units/seconds

    Example:
        Initialize a RateLimiter with the desired rate
            rate_limiter = RateLimiter(rate_limit)

        Now, every time before calling an operation, call acquire()
            await rate_limiter.acquire()

        And after an operation, update the number of units consumed
            rate_limiter.consume(units)
    """

    def __init__(self, rate_limit: float, time_module: Optional[Any] = None) -> None:
        if rate_limit <= 0:
            raise ValueError("rate_limit must be greater than zero")
        self._rate_limit = rate_limit
        self._consumed = 0
        self._time_of_last_acquire = 0.0
        self._time_module: Any = time_module or asyncio

    def consume(self, units: int) -> None:
        """
        Records the amount of units consumed.
        """
        self._consumed += units

    async def acquire(self) -> None:
        """
        Sleeps the appropriate amount of time to follow the rate limit restriction
        """
        sleep_time = max(0, self._consumed / float(self.rate_limit) -
                         (self._time_module.get_event_loop().time() - self._time_of_last_acquire))
        if sleep_time > 0:
            await self._time_module.sleep(sleep_time)
        self._consumed = 0
        self._time_of_last_acquire = self._time_module.get_event_loop().time()

    @property
    def rate_limit(self) -> float:
        return self._rate_limit

    @rate_limit.setter
    def rate_limit(self, rate_limit: float):
        if rate_limit <= 0:
            raise ValueError("rate_limit must be greater than zero")
        self._rate_limit = rate_limit


class PageIterator(AsyncIterator[_T]):
    """
    PageIterator handles Query and Scan result pagination.
    """

    def __init__(
        self,
        operation: Callable,
        args: Any,
        kwargs: Dict[str, Any],
        rate_limit: Optional[float] = None,
    ) -> None:
        self._operation = operation
        self._args = args
        self._kwargs = kwargs
        self._last_evaluated_key = kwargs.get('exclusive_start_key')
        self._is_last_page = False
        self._total_scanned_count = 0
        self._rate_limiter = None
        if rate_limit:
            self._rate_limiter = RateLimiter(rate_limit)

    def __aiter__(self) -> AsyncIterator[_T]:
        return self

    async def __anext__(self) -> _T:
        if self._is_last_page:
            raise StopAsyncIteration()

        self._kwargs['exclusive_start_key'] = self._last_evaluated_key

        if self._rate_limiter:
            await self._rate_limiter.acquire()
            self._kwargs['return_consumed_capacity'] = TOTAL

        page = await self._operation(*self._args, **self._kwargs)
        self._last_evaluated_key = page.get(LAST_EVALUATED_KEY)
        self._is_last_page = self._last_evaluated_key is None
        self._total_scanned_count += page[SCANNED_COUNT]

        if self._rate_limiter:
            consumed_capacity = page.get(CONSUMED_CAPACITY, {}).get(CAPACITY_UNITS, 0)
            self._rate_limiter.consume(consumed_capacity)

        return page

    @property
    def key_names(self) -> Iterable[str]:
        # If the current page has a last_evaluated_key, use it to determine key attributes
        if self._last_evaluated_key:
            return self._last_evaluated_key.keys()

        # Use the table meta data to determine the key attributes
        table_meta = self._operation.__self__.get_meta_table()  # type: ignore
        return table_meta.get_key_names(self._kwargs.get('index_name'))

    @property
    def page_size(self) -> Optional[int]:
        return self._kwargs.get('limit')

    @page_size.setter
    def page_size(self, page_size: int) -> None:
        self._kwargs['limit'] = page_size

    @property
    def last_evaluated_key(self) -> Optional[Dict[str, Dict[str, Any]]]:
        return self._last_evaluated_key

    @property
    def total_scanned_count(self) -> int:
        return self._total_scanned_count


class ResultIterator(AsyncIterator[_T]):
    """
    ResultIterator handles Query and Scan item pagination.
    """

    def __init__(
        self,
        operation: Callable,
        args: Any,
        kwargs: Dict[str, Any],
        map_fn: Optional[Callable] = None,
        limit: Optional[int] = None,
        rate_limit: Optional[float] = None,
    ) -> None:
        self.page_iter: PageIterator = PageIterator(operation, args, kwargs, rate_limit)
        self._map_fn = map_fn
        self._limit = limit
        self._total_count = 0
        self._index = 0
        self._count = 0
        self._items = None

    async def _get_next_page(self) -> None:
        page = await self.page_iter.__anext__()
        self._count = page[CAMEL_COUNT]
        self._items = page.get(ITEMS)  # not returned if 'Select' is set to 'COUNT'
        self._index = 0 if self._items else self._count
        self._total_count += self._count

    def __aiter__(self) -> AsyncIterator[_T]:
        return self

    async def __anext__(self) -> _T:
        if self._limit == 0:
            raise StopAsyncIteration

        while self._index == self._count:
            await self._get_next_page()

        item = self._items[self._index]
        self._index += 1
        if self._limit is not None:
            self._limit -= 1
        if self._map_fn:
            item = self._map_fn(item)
        return item

    @property
    def last_evaluated_key(self) -> Optional[Dict[str, Dict[str, Any]]]:
        if self._index == self._count:
            # Not started iterating yet: return `exclusive_start_key` if set, otherwise expect None; or,
            # Entire page has been consumed: last_evaluated_key is whatever DynamoDB returned
            return self.page_iter.last_evaluated_key

        # In the middle of a page of results: reconstruct a last_evaluated_key from the current item
        item = self._items[self._index - 1]
        return {key: item[key] for key in self.page_iter.key_names}

    @property
    def total_count(self) -> int:
        return self._total_count
