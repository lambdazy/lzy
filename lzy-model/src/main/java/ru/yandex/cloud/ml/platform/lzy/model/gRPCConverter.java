package ru.yandex.cloud.ml.platform.lzy.model;

import java.net.URI;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import ru.yandex.cloud.ml.platform.lzy.model.Context.ContextImpl;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.model.graph.*;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.*;
import yandex.cloud.priv.datasphere.v2.lzy.Channels;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.AmazonCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.AzureCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.AzureSASCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.GetS3CredentialsResponse;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks.ContextSpec;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks.SlotAssignment;

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
        } else if (env instanceof DockerEnv) {
            builder.setDocker(to((DockerEnv) env));
        }
        return builder.build();
    }


    public static Operations.PythonEnv to(PythonEnv env) {
        List<Operations.LocalModule> localModules = new ArrayList<>();
        env.localModules().forEach(localModule -> localModules.add(Operations.LocalModule.newBuilder()
                .setName(localModule.name())
                .setUri(localModule.uri())
                .build()));
        return Operations.PythonEnv.newBuilder()
                .setName(env.name())
                .setYaml(env.yaml())
                .addAllLocalModules(localModules)
                .build();
    }

    public static Operations.DockerEnv to(DockerEnv env) {
        return Operations.DockerEnv.newBuilder() // TODO lindvv
            .setUri(env.uri().toString())
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

    public static Env envFrom(Operations.Env env) {
        if (env.hasPyenv()) {
            return envFrom(env.getPyenv());
        }
        if (env.hasDocker()) {
            return envFrom(env.getDocker());
        }
        return null;
    }

    private static PythonEnv envFrom(Operations.PythonEnv env) {
        return new PythonEnvAdapter(env);
    }

    private static DockerEnv envFrom(Operations.DockerEnv env) {
        return new DockerEnvAdapter(env);
    }


    public static Channels.Channel to(Channel channel) {
        final Channels.Channel.Builder builder = Channels.Channel.newBuilder();
        builder.setChannelId(channel.name());
        builder.setContentType(to(channel.contentType()));
        return builder.build();
    }

    public static LzyWhiteboard.Snapshot to(Snapshot snapshot) {
        return LzyWhiteboard.Snapshot.newBuilder().setSnapshotId(snapshot.id().toString()).build();
    }

    public static Lzy.GetS3CredentialsResponse to(StorageCredentials credentials) {
        switch (credentials.type()){
            case Azure: return to((StorageCredentials.AzureCredentials) credentials);
            case AzureSas: return to((StorageCredentials.AzureSASCredentials) credentials);
            case Amazon: return to((StorageCredentials.AmazonCredentials)  credentials);
            default:
            case Empty:
                return GetS3CredentialsResponse.newBuilder().build();
        }
    }

    public static Lzy.GetS3CredentialsResponse to(StorageCredentials.AzureCredentials credentials){
        return GetS3CredentialsResponse.newBuilder()
            .setAzure(
                AzureCredentials.newBuilder()
                    .setConnectionString(credentials.connectionString())
                    .build()
            )
            .build();
    }

    public static Lzy.GetS3CredentialsResponse to(StorageCredentials.AmazonCredentials credentials){
        return GetS3CredentialsResponse.newBuilder()
            .setAmazon(AmazonCredentials.newBuilder()
                .setEndpoint(credentials.endpoint())
                .setAccessToken(credentials.accessToken())
                .setSecretToken(credentials.secretToken())
                .build())
            .build();
    }

    public static Context from(ContextSpec spec){
        return new ContextImpl(
            from(spec.getEnv()),
            from(spec.getProvisioning()),
            from(spec.getSnapshotMeta()),
            from(spec.getAssignmentsList())
        );
    }

    private static SnapshotMeta from(Tasks.SnapshotMeta snapshotMeta) {
        return SnapshotMeta.from(snapshotMeta);
    }

    private static List<Context.SlotAssignment> from(List<SlotAssignment> assignmentsList) {
        return assignmentsList.stream()
            .map(ass -> new Context.SlotAssignment(from(ass.getSlot()), ass.getBinding()))
            .collect(Collectors.toList());
    }

    private static Env from(Operations.Env env) {
        return envFrom(env);
    }

    public static Lzy.GetS3CredentialsResponse to(StorageCredentials.AzureSASCredentials credentials){
        return GetS3CredentialsResponse.newBuilder()
            .setAzureSas(
                AzureSASCredentials.newBuilder()
                    .setSignature(credentials.signature())
                    .setEndpoint(credentials.endpoint())
                    .build()
            )
            .build();
    }

    public static LzyWhiteboard.WhiteboardField to(
            WhiteboardField field, List<WhiteboardField> dependent, boolean empty, String storage) {
        return LzyWhiteboard.WhiteboardField.newBuilder()
                .setFieldName(field.name())
                .setStorageUri(storage)
                .addAllDependentFieldNames(dependent.stream().map(WhiteboardField::name).collect(Collectors.toList()))
                .setEmpty(empty)
                .build();
    }

    public static SnapshotEntry from(LzyWhiteboard.SnapshotEntry entry, Snapshot snapshot) {
        return new SnapshotEntry.Impl(entry.getEntryId(), snapshot);
    }

    public static LzyWhiteboard.WhiteboardStatus to(WhiteboardStatus.State state) {
        switch (state) {
            case CREATED:
                return LzyWhiteboard.WhiteboardStatus.CREATED;
            case COMPLETED:
                return LzyWhiteboard.WhiteboardStatus.COMPLETED;
            case NOT_COMPLETED:
                return LzyWhiteboard.WhiteboardStatus.NOT_COMPLETED;
            case ERRORED:
                return LzyWhiteboard.WhiteboardStatus.ERRORED;
            default:
                throw new IllegalArgumentException("Unknown state: " + state);
        }
    }

    public static LzyWhiteboard.WhiteboardInfo to(WhiteboardInfo wbInfo) {
        return LzyWhiteboard.WhiteboardInfo.newBuilder()
                .setId(wbInfo.id().toString())
                .setWhiteboardStatus(to(wbInfo.state()))
                .build();
    }

    private static class AtomicZygoteAdapter implements AtomicZygote {
        private final Operations.Zygote operation;

        AtomicZygoteAdapter(Operations.Zygote operation) {
            this.operation = operation;
        }

        @Override
        public void run() {
        }

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
        public String description() {
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
            return obj instanceof Slot && ((Slot) obj).name().equals(s.getName());
        }
    }

    private static class SlotStatusAdapter implements SlotStatus {
        private final Operations.SlotStatus slotStatus;

        SlotStatusAdapter(Operations.SlotStatus slotStatus) {
            this.slotStatus = slotStatus;
        }

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

    private static class LocalModuleAdapter implements LocalModule {
        private final String name;
        private final String uri;

        public LocalModuleAdapter(Operations.LocalModule localModule) {
            this.name = localModule.getName();
            this.uri = localModule.getUri();
        }

        public String name() {
            return name;
        }

        public String uri() {
            return uri;
        }
    }

    private static class PythonEnvAdapter implements PythonEnv {
        private final Operations.PythonEnv env;
        private final List<LocalModule> localModules;

        public PythonEnvAdapter(Operations.PythonEnv env) {
            this.env = env;
            localModules = new ArrayList<>();
            env.getLocalModulesList()
                    .forEach(localModule -> localModules.add(new LocalModuleAdapter(localModule)));
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
        public List<LocalModule> localModules() {
            return new ArrayList<>(localModules);
        }

        @Override
        public URI uri() {
            return URI.create("conda/" + name());
        }
    }

    private static class DockerEnvAdapter implements DockerEnv {
        private final Operations.DockerEnv env;

        public DockerEnvAdapter(Operations.DockerEnv env) {
            this.env = env;
        }

        // TODO lindvv

        @Override
        public URI uri() {
            return URI.create(env.getUri());
        }

        @Override
        public String name() {
            return env.getName();
        }
    }
}
