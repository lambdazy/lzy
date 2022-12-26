import asyncio
from threading import Thread
from typing import Optional, Awaitable, TypeVar, Sequence

T = TypeVar("T")


class LzyEventLoop:
    """
    Singleton to provide asyncio event loop to all lzy parts
    """
    instance: Optional["LzyEventLoop"] = None

    @staticmethod
    def __run_loop_thread(loop: asyncio.AbstractEventLoop):
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def __init__(self):
        self.__loop = asyncio.new_event_loop()
        self.__loop_thread = Thread(
            name="workflow-thread",
            target=self.__run_loop_thread,
            args=(self.__loop,),
            daemon=True,
        )
        self.__loop_thread.start()

    @classmethod
    def run_async(cls, fun: Awaitable[T]) -> T:
        if cls.instance is not None:
            inst = cls.instance
        else:
            inst = cls()
            cls.instance = inst
        return asyncio.run_coroutine_threadsafe(fun, inst.__loop).result()

    @classmethod
    def gather(cls, *args: Awaitable[T]) -> Sequence[T]:
        return cls.run_async(cls.__gather(*args))

    @staticmethod
    async def __gather(*args: Awaitable[T]) -> Sequence[T]:
        return await asyncio.gather(*args)
