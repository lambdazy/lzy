import asyncio
import dataclasses
import logging
from asyncio import Queue, Task, CancelledError
from typing import Union, Optional, AsyncIterator, Callable, Awaitable

from grpc.aio import insecure_channel, Channel

from lzy.api.v2.slots.slot import Slot, SlotDirection
from lzy.proto.priv.v2.lzy_fs_pb2 import SlotCommandStatus, SlotRequest, Message
from lzy.proto.priv.v2.lzy_kharon_grpc import LzyKharonStub
from lzy.proto.priv.v2.lzy_kharon_pb2 import ServerCommand, AttachTerminal, TerminalResponse, TerminalCommand, \
    SendSlotDataMessage
from lzy.proto.priv.v2.lzy_servant_pb2 import SlotAttach, SlotDetach
from lzy.proto.priv.v2 import lzy_zygote_pb2


@dataclasses.dataclass
class CommandResult:
    description: str = "Ok"
    is_success: bool = True

    @staticmethod
    def error(description: str) -> 'CommandResult':
        return CommandResult(description, False)


@dataclasses.dataclass
class ConnectSlot:
    slot_name: str
    slot_url: str


@dataclasses.dataclass
class DisconnectSlot:
    slot_name: str


@dataclasses.dataclass
class SlotStatus:
    slot_name: str


@dataclasses.dataclass
class DestroySlot:
    slot_name: str


SlotCommand = Union[ConnectSlot, DisconnectSlot, SlotStatus, DestroySlot]


log = logging.getLogger(__name__)


def build_response(command_id: str, description: str = "Ok", success: bool = True) -> ServerCommand:
    return ServerCommand(
        terminalResponse=TerminalResponse(
            commandId=command_id,
            slotStatus=SlotCommandStatus(
                rc=SlotCommandStatus.RC(
                    code=SlotCommandStatus.RC.SUCCESS if success else SlotCommandStatus.RC.ERROR,
                    description=description
                )
            )
        )
    )


async def input_stream_mapper(stream: AsyncIterator[Message]):
    async for msg in stream:
        if msg.HasField('control'):
            if msg.control == Message.Controls.EOS:
                return
        yield msg.chunk


async def output_stream_mapper(slot_uri: str, stream: AsyncIterator[bytes]):
    yield SendSlotDataMessage(
        request=SlotRequest(
        slotUri=slot_uri,
        offset=0
    ))
    async for msg in stream:
        yield SendSlotDataMessage(message=Message(chunk=msg))
    yield SendSlotDataMessage(message=Message(control=Message.Controls.EOS))


class KharonApi:

    # TODO(artolord) add auth
    def __init__(self, addr: str):
        self.__task: Optional[Task] = None
        self.__addr = addr
        self.__connected = False
        self.__response_queue: Queue[TerminalResponse] = Queue()
        self.__channel: Channel = insecure_channel(self.__addr)
        self.__kharon_stub: LzyKharonStub = LzyKharonStub(self.__channel)

    async def connect(self, progress_processor: Callable[[SlotCommand], Awaitable[CommandResult]]):
        if self.__connected:
            raise RuntimeError("Already connected to kharon")
        await self.__channel.channel_ready()
        progress: AsyncIterator[TerminalCommand] = await self.__kharon_stub.AttachTerminal(self.__return_stream())
        self.__task = asyncio.create_task(self.__coroutine(progress, progress_processor))
        self.__connected = True

    async def attach(self, slot: Slot, uri: str):  # TODO(artolord) rewrite with new channel manager
        if not self.__connected:
            raise RuntimeError("Not connected to kharon")
        await self.__response_queue.put(ServerCommand(attach=SlotAttach(
            slot=lzy_zygote_pb2.Slot(
                name=slot.name,
                direction=lzy_zygote_pb2.Slot.Direction.INPUT
                if slot.direction == SlotDirection.INPUT else lzy_zygote_pb2.Slot.Direction.OUTPUT
            ),
            channel=slot.channel_name,
            uri=uri
        )))

    async def detach(self, slot: Slot, uri: str):  # TODO(artolord) rewrite with new channel manager
        if not self.__connected:
            raise RuntimeError("Not connected to kharon")
        await self.__response_queue.put(ServerCommand(detach=SlotDetach(
            slot=lzy_zygote_pb2.Slot(
                name=slot.name,
                direction=lzy_zygote_pb2.Slot.Direction.INPUT
                if slot.direction == SlotDirection.INPUT else lzy_zygote_pb2.Slot.Direction.OUTPUT
            ),
            uri=uri
        )))

    async def open_output_slot(self, uri: str) -> AsyncIterator[bytes]:
        return input_stream_mapper(self.__kharon_stub.OpenOutputSlot(SlotRequest(slotUri=uri, offset=0)))

    async def write_to_input_slot(self, uri: str, stream: AsyncIterator[bytes]):
        await self.__kharon_stub.WriteToInputSlot(output_stream_mapper(uri, stream))

    async def __coroutine(self, progress: AsyncIterator[TerminalCommand],
                          progress_processor: Callable[[SlotCommand], Awaitable[CommandResult]]):
        if not self.__connected:
            raise RuntimeError("Not connected to kharon")
        try:
            async for command in progress:
                if command.HasField("connectSlot"):
                    await self.__response_queue.put(build_response(command.commandId, "Not implemented", False))
                    continue

                if command.HasField("connectSlot"):
                    res = await progress_processor(ConnectSlot(command.connectSlot.slotName,
                                                               command.connectSlot.slotUri))
                    await self.__response_queue.put(build_response(command.commandId, res.description, res.is_success))

                if command.HasField("destroySlot"):
                    res = await progress_processor(DestroySlot(command.destroySlot.slotName))
                    await self.__response_queue.put(build_response(command.commandId, res.description, res.is_success))

                if command.HasField("disconnectSlot"):
                    res = await progress_processor(DisconnectSlot(command.disconnectSlot.slotName))
                    await self.__response_queue.put(build_response(command.commandId, res.description, res.is_success))

                if command.HasField("statusSlot"):  # TODO(artolord) maybe add status response here
                    res = await progress_processor(SlotStatus(command.statusSlot.slotName))
                    await self.__response_queue.put(build_response(command.commandId, res.description, res.is_success))
        except CancelledError as e:
            log.info("Coroutine is stopping", e)

    async def __return_stream(self):
        yield ServerCommand(attachTerminal=AttachTerminal())
        while True:
            yield await self.__response_queue.get()

    async def close(self):
        self.__task.cancel("Cancelling coroutine")
