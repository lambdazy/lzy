"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import ai.lzy.v1.common.data_scheme_pb2
import builtins
import google.protobuf.descriptor
import google.protobuf.internal.enum_type_wrapper
import google.protobuf.message
import sys
import typing

if sys.version_info >= (3, 10):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing_extensions.final
class Slot(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class _Media:
        ValueType = typing.NewType("ValueType", builtins.int)
        V: typing_extensions.TypeAlias = ValueType

    class _MediaEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[Slot._Media.ValueType], builtins.type):  # noqa: F821
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        UNSPECIFIED: Slot._Media.ValueType  # 0
        FILE: Slot._Media.ValueType  # 1
        PIPE: Slot._Media.ValueType  # 2
        ARG: Slot._Media.ValueType  # 3

    class Media(_Media, metaclass=_MediaEnumTypeWrapper): ...
    UNSPECIFIED: Slot.Media.ValueType  # 0
    FILE: Slot.Media.ValueType  # 1
    PIPE: Slot.Media.ValueType  # 2
    ARG: Slot.Media.ValueType  # 3

    class _Direction:
        ValueType = typing.NewType("ValueType", builtins.int)
        V: typing_extensions.TypeAlias = ValueType

    class _DirectionEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[Slot._Direction.ValueType], builtins.type):  # noqa: F821
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        UNKNOWN: Slot._Direction.ValueType  # 0
        INPUT: Slot._Direction.ValueType  # 1
        OUTPUT: Slot._Direction.ValueType  # 2

    class Direction(_Direction, metaclass=_DirectionEnumTypeWrapper): ...
    UNKNOWN: Slot.Direction.ValueType  # 0
    INPUT: Slot.Direction.ValueType  # 1
    OUTPUT: Slot.Direction.ValueType  # 2

    NAME_FIELD_NUMBER: builtins.int
    MEDIA_FIELD_NUMBER: builtins.int
    DIRECTION_FIELD_NUMBER: builtins.int
    CONTENTTYPE_FIELD_NUMBER: builtins.int
    name: builtins.str
    media: global___Slot.Media.ValueType
    direction: global___Slot.Direction.ValueType
    @property
    def contentType(self) -> ai.lzy.v1.common.data_scheme_pb2.DataScheme: ...
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        media: global___Slot.Media.ValueType = ...,
        direction: global___Slot.Direction.ValueType = ...,
        contentType: ai.lzy.v1.common.data_scheme_pb2.DataScheme | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["contentType", b"contentType"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["contentType", b"contentType", "direction", b"direction", "media", b"media", "name", b"name"]) -> None: ...

global___Slot = Slot

@typing_extensions.final
class SlotStatus(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class _State:
        ValueType = typing.NewType("ValueType", builtins.int)
        V: typing_extensions.TypeAlias = ValueType

    class _StateEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[SlotStatus._State.ValueType], builtins.type):  # noqa: F821
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        UNSPECIFIED: SlotStatus._State.ValueType  # 0
        UNBOUND: SlotStatus._State.ValueType  # 1
        PREPARING: SlotStatus._State.ValueType  # 2
        OPEN: SlotStatus._State.ValueType  # 3
        SUSPENDED: SlotStatus._State.ValueType  # 4
        CLOSED: SlotStatus._State.ValueType  # 5
        DESTROYED: SlotStatus._State.ValueType  # 6

    class State(_State, metaclass=_StateEnumTypeWrapper):
        """For input slot SUSPENDED command could happen while reading to change a source of data
        For output slot CLOSED means that process has finished generating data.
        CLOSED and SUSPENDED are independent events: one could happen before or after the other. If CLOSED come while
        slot is in SUSPENDED state, slot must remember this event and after reconnection feed CLOSED back to the server
        """

    UNSPECIFIED: SlotStatus.State.ValueType  # 0
    UNBOUND: SlotStatus.State.ValueType  # 1
    PREPARING: SlotStatus.State.ValueType  # 2
    OPEN: SlotStatus.State.ValueType  # 3
    SUSPENDED: SlotStatus.State.ValueType  # 4
    CLOSED: SlotStatus.State.ValueType  # 5
    DESTROYED: SlotStatus.State.ValueType  # 6

    TASKID_FIELD_NUMBER: builtins.int
    DECLARATION_FIELD_NUMBER: builtins.int
    CONNECTEDTO_FIELD_NUMBER: builtins.int
    POINTER_FIELD_NUMBER: builtins.int
    STATE_FIELD_NUMBER: builtins.int
    taskId: builtins.str
    @property
    def declaration(self) -> global___Slot: ...
    connectedTo: builtins.str
    pointer: builtins.int
    state: global___SlotStatus.State.ValueType
    def __init__(
        self,
        *,
        taskId: builtins.str = ...,
        declaration: global___Slot | None = ...,
        connectedTo: builtins.str = ...,
        pointer: builtins.int = ...,
        state: global___SlotStatus.State.ValueType = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["declaration", b"declaration"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["connectedTo", b"connectedTo", "declaration", b"declaration", "pointer", b"pointer", "state", b"state", "taskId", b"taskId"]) -> None: ...

global___SlotStatus = SlotStatus

@typing_extensions.final
class SlotInstance(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TASKID_FIELD_NUMBER: builtins.int
    SLOT_FIELD_NUMBER: builtins.int
    SLOTURI_FIELD_NUMBER: builtins.int
    CHANNELID_FIELD_NUMBER: builtins.int
    taskId: builtins.str
    @property
    def slot(self) -> global___Slot: ...
    slotUri: builtins.str
    channelId: builtins.str
    def __init__(
        self,
        *,
        taskId: builtins.str = ...,
        slot: global___Slot | None = ...,
        slotUri: builtins.str = ...,
        channelId: builtins.str = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["slot", b"slot"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["channelId", b"channelId", "slot", b"slot", "slotUri", b"slotUri", "taskId", b"taskId"]) -> None: ...

global___SlotInstance = SlotInstance
