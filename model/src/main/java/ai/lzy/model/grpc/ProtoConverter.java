package ai.lzy.model.grpc;

import ai.lzy.model.DataScheme;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.model.slot.SlotStatus;
import ai.lzy.v1.common.LMD;
import ai.lzy.v1.common.LMS;
import jakarta.annotation.Nullable;

import java.net.URI;

public class ProtoConverter {

    public static Slot fromProto(LMS.Slot grpcSlot) {
        return new SlotAdapter(grpcSlot);
    }

    public static SlotStatus fromProto(LMS.SlotStatus slotStatus) {
        return new SlotStatusAdapter(slotStatus);
    }

    public static SlotInstance fromProto(LMS.SlotInstance slotInstance) {
        return new SlotInstance(
            fromProto(slotInstance.getSlot()),
            slotInstance.getTaskId(),
            slotInstance.getChannelId(),
            URI.create(slotInstance.getSlotUri())
        );
    }

    public static DataScheme fromProto(LMD.DataScheme dataScheme) {
        return new DataScheme(dataScheme.getDataFormat(), dataScheme.getSchemeFormat(),
            dataScheme.getSchemeContent(), dataScheme.getMetadataMap());
    }

    public static LMS.Slot toProto(Slot slot) {
        return LMS.Slot.newBuilder()
            .setName(slot.name())
            .setMedia(LMS.Slot.Media.valueOf(slot.media().name()))
            .setDirection(LMS.Slot.Direction.valueOf(slot.direction().name()))
            .setContentType(toProto(slot.contentType()))
            .build();
    }

    public static LMS.Slot.Media toProto(Slot.Media media) {
        return switch (media) {
            case FILE -> LMS.Slot.Media.FILE;
            case PIPE -> LMS.Slot.Media.PIPE;
            case ARG -> LMS.Slot.Media.ARG;
        };
    }

    public static LMD.DataScheme toProto(DataScheme dataScheme) {
        return LMD.DataScheme.newBuilder()
            .setDataFormat(dataScheme.dataFormat())
            .setSchemeFormat(dataScheme.schemeFormat())
            .setSchemeContent(dataScheme.schemeContent())
            .putAllMetadata(dataScheme.metadata())
            .build();
    }

    public static LMS.SlotInstance toProto(SlotInstance slotInstance) {
        return LMS.SlotInstance.newBuilder()
            .setSlot(ProtoConverter.toProto(slotInstance.spec()))
            .setTaskId(slotInstance.taskId())
            .setChannelId(slotInstance.channelId())
            .setSlotUri(slotInstance.uri().toString())
            .build();
    }

    public static LMS.Slot buildFileInputPlainContentSlot(String slotName) {
        return buildFileInputSlot(slotName, toProto(DataScheme.PLAIN));
    }

    public static LMS.Slot buildFileOutputPlainContentSlot(String slotName) {
        return buildFileOutputSlot(slotName, toProto(DataScheme.PLAIN));
    }

    public static LMS.Slot buildFileInputSlot(String slotName, LMD.DataScheme scheme) {
        return buildFileSlot(slotName, LMS.Slot.Direction.INPUT, scheme);
    }

    public static LMS.Slot buildFileOutputSlot(String slotName, LMD.DataScheme scheme) {
        return buildFileSlot(slotName, LMS.Slot.Direction.OUTPUT, scheme);
    }

    public static LMS.Slot buildFileSlot(String slotName, LMS.Slot.Direction direction, LMD.DataScheme scheme) {
        return LMS.Slot.newBuilder()
            .setName(slotName)
            .setDirection(direction)
            .setContentType(scheme)
            .setMedia(LMS.Slot.Media.FILE)
            .build();
    }

    public static class SlotAdapter implements Slot {

        private final LMS.Slot s;

        public SlotAdapter(LMS.Slot s) {
            this.s = s;
        }

        @Override
        public String name() {
            return s.getName();
        }

        @Override
        public Media media() {
            return Media.valueOf(s.getMedia().name());
        }

        @Override
        public Direction direction() {
            return Direction.valueOf(s.getDirection().name());
        }

        @Override
        public @Nullable
        DataScheme contentType() {
            return fromProto(s.getContentType());
        }

        @Override
        public int hashCode() {
            return s.getName().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Slot && ((Slot) obj).name().equals(s.getName());
        }
    }

    private static class SlotStatusAdapter implements SlotStatus {

        private final LMS.SlotStatus slotStatus;

        SlotStatusAdapter(LMS.SlotStatus slotStatus) {
            this.slotStatus = slotStatus;
        }

        @Nullable
        @Override
        public String channelId() {
            return slotStatus.getConnectedTo();
        }

        @Override
        public String tid() {
            slotStatus.getTaskId();
            return !slotStatus.getTaskId().isEmpty() ? slotStatus.getTaskId() : null;
        }

        @Override
        public Slot slot() {
            return fromProto(slotStatus.getDeclaration());
        }

        @Override
        public URI connected() {
            return slotStatus.getConnectedTo().isEmpty() ? null
                : URI.create(slotStatus.getConnectedTo());
        }

        @Override
        public long pointer() {
            return slotStatus.getPointer();
        }

        @Override
        public State state() {
            return State.valueOf(slotStatus.getState().name());
        }
    }


}
