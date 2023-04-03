import asyncio
import logging
import weakref
from io import TextIOWrapper
from typing import TYPE_CHECKING, Callable, Coroutine

from ._asyncio_loop import get_running_loop, get_task_loop
from ._error_interceptor import ErrorInterceptor

if TYPE_CHECKING:
    from loguru import Message
else:
    from ._handler import Message


class StreamSink:
    def __init__(self, stream: TextIOWrapper) -> None:
        self._stream = stream
        self._flushable = callable(getattr(stream, "flush", None))
        self._stoppable = callable(getattr(stream, "stop", None))
        self._completable = asyncio.iscoroutinefunction(getattr(stream, "complete", None))

    def write(self, message: Message) -> None:
        self._stream.write(message)
        if self._flushable:
            self._stream.flush()  # type: ignore[attr-defined]

    def stop(self) -> None:
        if self._stoppable:
            self._stream.stop()  # type: ignore[attr-defined]

    async def complete(self) -> None:
        if self._completable:
            await self._stream.complete()  # type: ignore[attr-defined]


class StandardSink:
    def __init__(self, handler: logging.Handler) -> None:
        self._handler = handler

    def write(self, message: Message) -> None:
        record = message.record
        message_str = str(message)
        exc = record["exception"]
        new_record = logging.getLogger().makeRecord(
            record["name"] or "",
            record["level"].no,
            record["file"].path,
            record["line"],
            message_str,
            (),
            (exc.type, exc.value, exc.traceback) if exc else None,  # type: ignore[arg-type]
            record["function"],
            {"extra": record["extra"]},
        )
        if exc:
            new_record.exc_text = "\n"
        self._handler.handle(new_record)

    def stop(self) -> None:
        self._handler.close()

    async def complete(self) -> None:
        pass


class AsyncSink:
    def __init__(
        self,
        function: Callable,
        loop: asyncio.SelectorEventLoop,
        error_interceptor: ErrorInterceptor,
    ):
        self._function = function
        self._loop = loop
        self._error_interceptor = error_interceptor
        self._tasks: weakref.WeakSet = weakref.WeakSet()

    def write(self, message: Message) -> None:
        try:
            loop = self._loop or get_running_loop()
        except RuntimeError:
            return

        coroutine: Coroutine = self._function(message)

        task: asyncio.Task = loop.create_task(coroutine)

        def check_exception(future: asyncio.Task) -> None:
            if future.cancelled() or future.exception() is None:
                return
            if not self._error_interceptor.should_catch():
                raise future.exception()  # type: ignore[misc]
            self._error_interceptor.print(message.record, exception=future.exception())

        task.add_done_callback(check_exception)
        self._tasks.add(task)

    def stop(self) -> None:
        for task in self._tasks:
            task.cancel()

    async def complete(self) -> None:
        loop = get_running_loop()
        for task in self._tasks:
            if get_task_loop(task) is loop:
                try:
                    await task
                except Exception:
                    pass  # Handled in "check_exception()"

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_tasks"] = None
        return state

    def __setstate__(self, state: dict):
        self.__dict__.update(state)
        self._tasks = weakref.WeakSet()


class CallableSink:
    def __init__(self, function: Callable) -> None:
        self._function = function

    def write(self, message: Message) -> None:
        self._function(message)

    def stop(self):
        pass

    async def complete(self):
        pass
