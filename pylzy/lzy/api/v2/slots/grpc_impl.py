import logging
from typing import Optional, AsyncIterator, Mapping, Dict

from lzy.api.v2.slots.kharon_api import KharonApi, CommandResult, SlotCommand, ConnectSlot, DisconnectSlot,\
    DestroySlot, SlotStatus
from lzy.api.v2.slots.slot import SlotManager, InputSlot, Slot, OutputSlot
from lzy.proto.priv.v2.lzy_fs_pb2 import ConnectSlotRequest

from lzy.proto.priv.v2.lzy_kharon_pb2 import AttachTerminal, TerminalResponse, TerminalCommand, ServerCommand
from asyncio import Queue, Event, Lock, Future

log = logging.getLogger(__name__)


class GrpcInputSlot(InputSlot):
    def __init__(self, slot_name: str, channel_name: str, kharon: KharonApi, slot_manager: 'GrpcSlotManager'):
        super(GrpcInputSlot, self).__init__(slot_name, channel_name)
        self.__connected: Future[AsyncIterator[bytes]] = Future()
        self.__buffer: bytes = b""
        self.__read_lock = Lock()
        self.__kharon = kharon
        self.__slot_manager = slot_manager

    async def read(self, num: int) -> Optional[bytes]:
        data_stream = await self.__connected
        await self.__read_lock.acquire()
        try:
            bytes_to_read = num
            ret = self.__buffer
            bytes_to_read -= len(self.__buffer)
            if bytes_to_read < 0:
                self.__buffer = ret[bytes_to_read:]
                ret = ret[:bytes_to_read]

            while bytes_to_read > 0:
                try:
                    data = await anext(data_stream)
                    ret += data
                    bytes_to_read -= len(data)
                    if bytes_to_read < 0:
                        self.__buffer = ret[bytes_to_read:]
                        ret = ret[:bytes_to_read]
                except StopAsyncIteration:
                    if len(ret) > 0:
                        return ret
                    return None
            return ret
        finally:
            self.__read_lock.release()

    async def connect(self, slot_addr: str):
        self.__connected.set_result(await self.__kharon.open_output_slot(slot_addr))

    async def close(self):
        log.info(f"Slot {self.name} closing by user")
        if not self.__connected.done():
            self.__connected.set_exception(RuntimeError("Closed by user, cannot read"))
        await self.__kharon.detach(self, self.uri)
        self.__slot_manager._notify_destroyed(self.name)

    async def suspend(self):
        log.info(f"Suspending slot {self.name}")
        await self.close()

    async def destroy(self):
        log.info(f"Destroying slot {self.name}")
        await self.close()


class GrpcOutputSlot(OutputSlot):
    def __init__(self, slot_name: str, channel_name: str, kharon: KharonApi, slot_manager: 'GrpcSlotManager'):
        super(GrpcOutputSlot, self).__init__(slot_name, channel_name)
        self.__connected: Future[AsyncIterator[bytes]] = Future()
        self.__buffer: bytes = b""
        self.__read_lock = Lock()
        self.__kharon = kharon
        self.__slot_manager = slot_manager


class GrpcSlotManager(SlotManager):

    def __init__(self, kharon: KharonApi):
        self.__slots: Dict[str, Slot] = {}
        self.__kharon = kharon

    async def connect(self):
        await self.__kharon.connect(self.__progress_processor)

    async def __connect_slot(self, slot_name: str, uri: str) -> CommandResult:
        slot = self.__slots.get(slot_name)
        if slot is None:
            return CommandResult.error("Slot not found")
        if isinstance(slot, GrpcInputSlot):
            await slot.connect(uri)
            return CommandResult()
        else:
            return CommandResult.error("Slot is output, connect not supported")

    async def __disconnect_slot(self, slot_name: str) -> CommandResult:
        slot = self.__slots.get(slot_name)
        if slot is None:
            return CommandResult.error("Slot not found")
        await slot.suspend()

    async def __destroy_slot(self, slot_name: str) -> CommandResult:
        slot = self.__slots.get(slot_name)
        if slot is None:
            return CommandResult.error("Slot not found")
        await slot.destroy()

    async def __progress_processor(self, command: SlotCommand) -> CommandResult:
        if isinstance(command, ConnectSlot):
            return await self.__connect_slot(command.slot_name, command.slot_url)
        if isinstance(command, DisconnectSlot):
            return await self.__disconnect_slot(command.slot_name)
        if isinstance(command, DestroySlot):
            return await self.__destroy_slot(command.slot_name)
        if isinstance(command, SlotStatus):
            return CommandResult()  # TODO(artolord) implement this
        return CommandResult.error("Not supported")

    async def close(self):
        pass

    async def open_input(self, channel_name: str, slot_name: str) -> InputSlot:
        pass

    async def open_output(self, channel_name: str, slot_name: str) -> InputSlot:
        pass

    def _notify_destroyed(self, slot_name: str):
        self.__slots.pop(slot_name)
