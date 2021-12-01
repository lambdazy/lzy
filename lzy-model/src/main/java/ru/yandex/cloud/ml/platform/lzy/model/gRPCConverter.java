package ru.yandex.cloud.ml.platform.lzy.model;

import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import ru.yandex.cloud.ml.platform.lzy.model.graph.PythonEnv;
import yandex.cloud.priv.datasphere.v2.lzy.Channels;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class gRPCConverter {
    public static Zygote from(Operations.Zygote grpcOperation) {
        return new AtomicZygoteAdapter(grpcOperation);
    }

    public static Operations.Zygote to(Zygote zygote) {
        final Operations.Zygote.Builder builder = Operations.Zygote.newBuilder();
        if (zygote instanceof AtomicZygote) {
            final AtomicZygote atomicZygote = (AtomicZygote) zygote;
            builder.setEnv(to(atomicZygote.env()));
            builder.setProvisioning(to(atomicZygote.provisioning()));
            builder.setFuze(atomicZygote.fuze());
            Stream.concat(Stream.of(atomicZygote.input()), Stream.of(atomicZygote.output()))
                    .forEach(slot -> builder.addSlots(to(slot)));
        }
        return builder.build();
    }

    public static Operations.Env to(Env env) {
        Operations.Env.Builder builder = Operations.Env.newBuilder();
        if (env instanceof PythonEnv) {
            builder.setPyenv(to((PythonEnv) env));
        } // else if (env instanceof DockerEnv) {...}
        return builder.build();
    }


    public static Operations.PythonEnv to(PythonEnv env) {
        return Operations.PythonEnv.newBuilder()
                .setName(env.name())
                .setYaml(env.yaml())
                .build();
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

    public static String to(DataSchema contentType) {
        return "not implemented yet";
    }

    public static DataSchema contentTypeFrom(String contentTypeJson) {
        return null;
    }

    private static Operations.Provisioning to(Provisioning provisioning) {
        return Operations.Provisioning.newBuilder()
                .addAllTags(provisioning.tags()
                        .map(tag -> Operations.Provisioning.Tag.newBuilder().setTag(tag.tag()).build())
                        .collect(Collectors.toList()))
                .build();
    }

    private static Provisioning from(Operations.Provisioning provisioning) {
        return () -> provisioning.getTagsList().stream().map(tag -> (Provisioning.Tag) tag::getTag);
    }

    private static Env envFrom(Operations.Env env) {
        if (env.hasPyenv()) {
            return envFrom(env.getPyenv());
        }
        return null;
    }

    private static PythonEnv envFrom(Operations.PythonEnv env) {
        return new PythonEnvAdapter(env);
    }


    public static Channels.Channel to(Channel channel) {
        final Channels.Channel.Builder builder = Channels.Channel.newBuilder();
        builder.setChannelId(channel.name());
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
        public Env env() {
            return envFrom(operation.getEnv());
        }

        @Override
        public Provisioning provisioning() {
            return from(operation.getProvisioning());
        }

        @Override
        public Operations.Zygote zygote() {
            return operation;
        }

        @Override
        public String fuze() {
            return operation.getFuze();
        }

        @Override
        public String description(){
            return operation.getDescription();
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

        @Override
        public int hashCode() {
            return s.getName().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Slot && ((Slot)obj).name().equals(s.getName());
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
        public String user() {
            return slotStatus.getUser();
        }

        @Override
        public UUID tid() {
            slotStatus.getTaskId();
            return !slotStatus.getTaskId().isEmpty() ? UUID.fromString(slotStatus.getTaskId()) : null;
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

    private static class PythonEnvAdapter implements PythonEnv {
        private final Operations.PythonEnv env;

        public PythonEnvAdapter(Operations.PythonEnv env) {
            this.env = env;
        }

        @Override
        public String name() {
            return this.env.getName();
        }

        @Override
        public String yaml() {
            return this.env.getYaml();
        }

        @Override
        public URI uri() {
            return URI.create("conda/" + name());
        }
    }
}
