import asyncio
from typing import TypeVar, Type

import anyio

from fair_async_rlock.base_fair_async_rlock import BaseFairAsyncRLock

__all__ = [
    'AnyIOFairAsyncRLock'
]
TaskType = TypeVar('TaskType')
EventType = TypeVar('EventType')


class AnyIOFairAsyncRLock(BaseFairAsyncRLock[anyio.TaskInfo, anyio.Event]):
    """
    A fair reentrant lock for async programming. Fair means that it respects the order of acquisition.
    """

    def _get_current_task(self) -> anyio.TaskInfo:
        return anyio.get_current_task()

    def _get_cancelled_exc_class(self) -> Type[BaseException]:
        return anyio.get_cancelled_exc_class()

    def _get_wake_event(self) -> asyncio.Event:
        return anyio.Event()
