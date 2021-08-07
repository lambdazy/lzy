package ru.yandex.cloud.ml.platform.lzy.model;

import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Container;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Channel;
import ru.yandex.cloud.ml.platform.lzy.server.task.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import yandex.cloud.priv.datasphere.v2.lzy.Channels;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.UUID;
import java.util.stream.Stream;

public abstract class gRPCConverter {
    public static Zygote from(Operations.Zygote grpcOperation) {
        return new AtomicZygoteAdapter(grpcOperation);
    }

    public static Operations.Zygote to(Zygote zygote) {
        final Operations.Zygote.Builder builder = Operations.Zygote.newBuilder();
        if (zygote instanceof AtomicZygote) {
            final AtomicZygote atomicZygote = (AtomicZygote) zygote;
            builder.setEnv(atomicZygote.container().uri().toString());
            builder.setProvisioning(to(atomicZygote.provisioning()));
            builder.setFuze(atomicZygote.fuze());
            Stream.concat(Stream.of(atomicZygote.input()), Stream.of(atomicZygote.output()))
                .forEach(slot -> builder.addSlots(to(slot)));
        }
        return builder.build();
    }

    public static Operations.Slot to(Slot slot) {
        return Operations.Slot.newBuilder()
            .setName(slot.name())
            .setMedia(Operations.Slot.Media.valueOf(slot.media().name()))
            .setDirection(Operations.Slot.Direction.valueOf(slot.direction().name()))
            .setContentType(to(slot.contentType()))
            .build();
    }

    public static Slot from(Operations.Slot grpcSlot) {
        return new SlotAdapter(grpcSlot);
    }

    public static SlotStatus from(Operations.SlotStatus slotStatus) {
        return new SlotStatusAdapter(slotStatus);
    }

    private static String to(DataSchema contentType) {
        return "not implemented yet";
    }

    public static DataSchema contentTypeFrom(String contentTypeJson) {
        return null;
    }

    private static String to(Provisioning provisioning) {
        return "not implemented yet";
    }

    private static Provisioning provisioningFrom(String contentType) {
        return null;
    }

    public static Channels.Channel to(Channel channel) {
        final Channels.Channel.Builder builder = Channels.Channel.newBuilder();
        builder.setChannelId(channel.name().toString());
        builder.setContentType(to(channel.contentType()));
        return builder.build();
    }

    private static class AtomicZygoteAdapter implements AtomicZygote {
        private final Operations.Zygote operation;

        AtomicZygoteAdapter(Operations.Zygote operation) {
            this.operation = operation;
        }

        @Override
        public void run() {}

        @Override
        public Slot[] input() {
            return operation.getSlotsList().stream()
                .filter(s -> s.getDirection() == Operations.Slot.Direction.INPUT)
                .map(SlotAdapter::new)
                .toArray(Slot[]::new);
        }

        @Override
        public Slot[] output() {
            return operation.getSlotsList().stream()
                .filter(s -> s.getDirection() == Operations.Slot.Direction.OUTPUT)
                .map(SlotAdapter::new)
                .toArray(Slot[]::new);
        }

        @Override
        public Provisioning provisioning() {
            return provisioningFrom(operation.getProvisioning());
        }


        @Override
        public Container container() {
            //noinspection Convert2Lambda
            return new Container() {
                @Override
                public URI uri() {
                    return URI.create(operation.getEnv());
                }
            };
        }

        @Override
        public String fuze() {
            return operation.getFuze();
        }
    }


    private static class SlotAdapter implements Slot {
        private final Operations.Slot s;

        SlotAdapter(Operations.Slot s) {
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
        DataSchema contentType() {
            return contentTypeFrom(s.getContentType());
        }
    }

    private static class SlotStatusAdapter implements SlotStatus {
        private final Operations.SlotStatus slotStatus;

        SlotStatusAdapter(Operations.SlotStatus slotStatus) {this.slotStatus = slotStatus;}

        @Nullable
        @Override
        public String channelId() {
            return slotStatus.getConnectedTo();
        }

        @Override
        public Task task() {
            return null;
        }

        @Override
        public Slot slot() {
            return from(slotStatus.getDeclaration());
        }

        @Override
        public URI connected() {
            return slotStatus.getConnectedTo().isEmpty() ? null : URI.create(slotStatus.getConnectedTo());
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
