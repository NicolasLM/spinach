import asyncio
from collections import deque
from concurrent.futures import Future
import threading
from typing import Tuple, Optional, Any, Deque


class Queuey:
    """Hybrid queue allowing to interface sync and async(io) code.

    It is widely inspired by a talk by David Beazley on the subject:
    https://www.youtube.com/watch?v=x1ndXuw7S0s

    One big difference with a normal queue is that even with a maxsize
    set to a fixed number, this queue can still end up taking an
    infinite amount of memory since pending get/put operation are kept
    as futures.

    It is an alternative to the 3rd-party Janus library which had
    shortcomings when used in Spinach:
    - Janus queues have to be created in an asyncio coroutine, turning
      the creation of the queue in the Workers class into a strange dance.
    - It was not obvious to me how to implement showing the queue as full
      if there are unfinished tasks.
    - It adds a few dependencies only needed by a fractions of users, adds a
      ton of code for something that should be simple.
    """

    def __init__(self, maxsize: int):
        self.maxsize = maxsize
        self._mutex = threading.Lock()
        self._items: Deque[Any] = deque()
        self._getters: Deque[Future] = deque()
        self._putters: Deque[Tuple[Any, Future]] = deque()
        self._unfinished_tasks = 0

    def _get_noblock(self) -> Tuple[Optional[Any], Optional[Future]]:
        with self._mutex:
            if self._items:
                if self._putters:
                    # About to remove one item from the queue which means
                    # that a new spot will be available. Since there are
                    # putters waiting, wake up one and take its item.
                    item, put_fut = self._putters.popleft()
                    self._items.append(item)
                    put_fut.set_result(True)
                return self._items.popleft(), None

            else:
                fut = Future()
                self._getters.append(fut)
                return None, fut

    def _put_noblock(self, item: Any) -> Optional[Future]:
        with self._mutex:
            if len(self._items) < self.maxsize:
                self._items.append(item)
                self._unfinished_tasks += 1
                if self._getters:
                    self._getters.popleft().set_result(self._items.popleft())
            else:
                fut = Future()
                self._putters.append((item, fut))
                return fut

    def get_sync(self) -> Any:
        item, fut = self._get_noblock()
        if fut:
            item = fut.result()
        return item

    def put_sync(self, item: Any) -> None:
        fut = self._put_noblock(item)
        if fut is None:
            return

        fut.result()

    async def get_async(self) -> Any:
        item, fut = self._get_noblock()
        if fut:
            item = await asyncio.wait_for(asyncio.wrap_future(fut), None)
        return item

    async def put_async(self, item: Any) -> None:
        fut = self._put_noblock(item)
        if fut is None:
            return

        await asyncio.wait_for(asyncio.wrap_future(fut), None)

    def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete.

        Raises a ValueError if called more times than there were items
        placed in the queue.
        """
        with self._mutex:
            unfinished = self._unfinished_tasks - 1
            if unfinished < 0:
                raise ValueError('task_done() called too many times')

            self._unfinished_tasks = unfinished

    def empty(self) -> bool:
        with self._mutex:
            return self._unfinished_tasks == 0

    def full(self) -> bool:
        with self._mutex:
            return self.maxsize <= self._unfinished_tasks

    def available_slots(self) -> int:
        with self._mutex:
            return self.maxsize - self._unfinished_tasks
