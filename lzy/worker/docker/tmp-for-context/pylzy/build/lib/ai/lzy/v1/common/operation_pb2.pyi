"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import ai.lzy.v1.common.env_pb2
import ai.lzy.v1.common.slot_pb2
import builtins
import collections.abc
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.message
import sys

if sys.version_info >= (3, 8):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing_extensions.final
class Operation(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    @typing_extensions.final
    class StdSlotDesc(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        NAME_FIELD_NUMBER: builtins.int
        CHANNELID_FIELD_NUMBER: builtins.int
        name: builtins.str
        channelId: builtins.str
        def __init__(
            self,
            *,
            name: builtins.str = ...,
            channelId: builtins.str = ...,
        ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal["channelId", b"channelId", "name", b"name"]) -> None: ...

    ENV_FIELD_NUMBER: builtins.int
    REQUIREMENTS_FIELD_NUMBER: builtins.int
    COMMAND_FIELD_NUMBER: builtins.int
    SLOTS_FIELD_NUMBER: builtins.int
    DESCRIPTION_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    STDOUT_FIELD_NUMBER: builtins.int
    STDERR_FIELD_NUMBER: builtins.int
    @property
    def env(self) -> ai.lzy.v1.common.env_pb2.EnvSpec: ...
    @property
    def requirements(self) -> global___Requirements: ...
    command: builtins.str
    @property
    def slots(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[ai.lzy.v1.common.slot_pb2.Slot]: ...
    description: builtins.str
    name: builtins.str
    @property
    def stdout(self) -> global___Operation.StdSlotDesc:
        """Std slots for stderr and stdout. Optional"""
    @property
    def stderr(self) -> global___Operation.StdSlotDesc: ...
    def __init__(
        self,
        *,
        env: ai.lzy.v1.common.env_pb2.EnvSpec | None = ...,
        requirements: global___Requirements | None = ...,
        command: builtins.str = ...,
        slots: collections.abc.Iterable[ai.lzy.v1.common.slot_pb2.Slot] | None = ...,
        description: builtins.str = ...,
        name: builtins.str = ...,
        stdout: global___Operation.StdSlotDesc | None = ...,
        stderr: global___Operation.StdSlotDesc | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["env", b"env", "requirements", b"requirements", "stderr", b"stderr", "stdout", b"stdout"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["command", b"command", "description", b"description", "env", b"env", "name", b"name", "requirements", b"requirements", "slots", b"slots", "stderr", b"stderr", "stdout", b"stdout"]) -> None: ...

global___Operation = Operation

@typing_extensions.final
class Requirements(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    POOLLABEL_FIELD_NUMBER: builtins.int
    ZONE_FIELD_NUMBER: builtins.int
    poolLabel: builtins.str
    zone: builtins.str
    def __init__(
        self,
        *,
        poolLabel: builtins.str = ...,
        zone: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["poolLabel", b"poolLabel", "zone", b"zone"]) -> None: ...

global___Requirements = Requirements

@typing_extensions.final
class SlotToChannelAssignment(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SLOTNAME_FIELD_NUMBER: builtins.int
    CHANNELID_FIELD_NUMBER: builtins.int
    slotName: builtins.str
    channelId: builtins.str
    def __init__(
        self,
        *,
        slotName: builtins.str = ...,
        channelId: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["channelId", b"channelId", "slotName", b"slotName"]) -> None: ...

global___SlotToChannelAssignment = SlotToChannelAssignment

@typing_extensions.final
class TaskDesc(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OPERATION_FIELD_NUMBER: builtins.int
    SLOTASSIGNMENTS_FIELD_NUMBER: builtins.int
    @property
    def operation(self) -> global___Operation: ...
    @property
    def slotAssignments(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___SlotToChannelAssignment]: ...
    def __init__(
        self,
        *,
        operation: global___Operation | None = ...,
        slotAssignments: collections.abc.Iterable[global___SlotToChannelAssignment] | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["operation", b"operation"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["operation", b"operation", "slotAssignments", b"slotAssignments"]) -> None: ...

global___TaskDesc = TaskDesc
