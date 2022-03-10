package ru.yandex.cloud.ml.platform.lzy.model;

import com.google.protobuf.Timestamp;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import ru.yandex.cloud.ml.platform.lzy.model.channel.ChannelSpec;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AuxEnv;
import ru.yandex.cloud.ml.platform.lzy.model.graph.BaseEnv;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.graph.LocalModule;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import ru.yandex.cloud.ml.platform.lzy.model.graph.PythonEnv;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.ExecutionArg;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.InputExecutionArg;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotExecution;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardField;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus;
import yandex.cloud.priv.datasphere.v2.lzy.Channels;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.AmazonCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.AzureCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.AzureSASCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.GetS3CredentialsResponse;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard.WhiteboardField.Builder;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard.WhiteboardField.Status;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks.ContextSpec;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks.SlotAssignment;

public abstract class GrpcConverter {

    public static Zygote from(Operations.Zygote grpcOperation) {
        return new AtomicZygoteAdapter(grpcOperation);
    }

    public static Slot from(Operations.Slot grpcSlot) {
        return new SlotAdapter(grpcSlot);
    }

    public static SlotStatus from(Operations.SlotStatus slotStatus) {
        return new SlotStatusAdapter(slotStatus);
    }

    private static Provisioning from(Operations.Provisioning provisioning) {
        return () -> provisioning.getTagsList().stream().map(tag -> (Provisioning.Tag) tag::getTag);
    }

    public static Env from(Operations.Env env) {
        final BaseEnv baseEnv;
        if (env.hasBaseEnv()) {
            baseEnv = from(env.getBaseEnv());
        } else {
            baseEnv = null;
        }
        final AuxEnv auxEnv;
        if (env.hasAuxEnv()) {
            auxEnv = from(env.getAuxEnv());
        } else {
            auxEnv = null;
        }
        return new EnvImpl(baseEnv, auxEnv);
    }

    public static BaseEnv from(Operations.BaseEnv env) {
        return new BaseEnvAdapter(env);
    }

    public static AuxEnv from(Operations.AuxEnv env) {
        if (env.hasPyenv()) {
            return from(env.getPyenv());
        }
        return null;
    }

    private static PythonEnv from(Operations.PythonEnv env) {
        return new PythonEnvAdapter(env);
    }

    public static SnapshotEntry from(LzyWhiteboard.SnapshotEntry entry, Snapshot snapshot) {
        return new SnapshotEntry.Impl(entry.getEntryId(), snapshot);
    }

    public static Context from(ContextSpec spec) {
        return new ContextImpl(
            from(spec.getEnv()),
            from(spec.getProvisioning()),
            from(spec.getAssignmentsList().stream())
        );
    }

    private static Stream<Context.SlotAssignment> from(Stream<SlotAssignment> assignmentsList) {
        return assignmentsList
            .map(ass -> new Context.SlotAssignment(from(ass.getSlot()), ass.getBinding()));
    }

    public static Date from(Timestamp date) {
        return Date.from(Instant.ofEpochSecond(date.getSeconds(), date.getNanos()));
    }

    public static SnapshotExecution from(LzyWhiteboard.ExecutionDescription description) {
        ArrayList<ExecutionArg> outputs = new ArrayList<>();
        ArrayList<InputExecutionArg> inputs = new ArrayList<>();
        SnapshotExecution execution = new SnapshotExecution.SnapshotExecutionImpl(
            description.getName(),
            description.getSnapshotId(),
            outputs,
            inputs
        );
        description.getOutputList().stream().map(arg ->
            new SnapshotExecution.ExecutionArgImpl(arg.getName(), execution.snapshotId(), arg.getEntryId())
        ).forEach(outputs::add);
        description.getInputList().stream().map(arg ->
            new SnapshotExecution.InputExecutionArgImpl(arg.getName(), execution.snapshotId(),
                arg.getEntryId(), arg.getHash())
        ).forEach(inputs::add);
        return execution;
    }

    public static DataSchema contentTypeFrom(String contentTypeJson) {
        return null;
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
        if (env != null) {
            if (env.baseEnv() != null) {
                builder.setBaseEnv(to(env.baseEnv()));
            }
            if (env.auxEnv() != null) {
                builder.setAuxEnv(to(env.auxEnv()));
            }
        }
        return builder.build();
    }

    public static Operations.BaseEnv to(BaseEnv env) {
        Operations.BaseEnv.Builder builder = Operations.BaseEnv.newBuilder();
        if (env.name() != null) {
            builder.setName(env.name());
        }
        return builder.build();
    }

    public static Operations.AuxEnv to(AuxEnv env) {
        Operations.AuxEnv.Builder builder = Operations.AuxEnv.newBuilder();
        if (env instanceof PythonEnv) {
            builder.setPyenv(to((PythonEnv) env));
        }
        return builder.build();
    }

    public static Operations.PythonEnv to(PythonEnv env) {
        List<Operations.LocalModule> localModules = new ArrayList<>();
        env.localModules()
            .forEach(localModule -> localModules.add(Operations.LocalModule.newBuilder()
                .setName(localModule.name())
                .setUri(localModule.uri())
                .build()));
        return Operations.PythonEnv.newBuilder()
            .setName(env.name())
            .setYaml(env.yaml())
            .addAllLocalModules(localModules)
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

    public static String to(DataSchema contentType) {
        return "not implemented yet";
    }

    private static Operations.Provisioning to(Provisioning provisioning) {
        return Operations.Provisioning.newBuilder()
            .addAllTags(provisioning.tags()
                .map(tag -> Operations.Provisioning.Tag.newBuilder().setTag(tag.tag()).build())
                .collect(Collectors.toList()))
            .build();
    }

    public static Channels.Channel to(ChannelSpec channel) {
        final Channels.Channel.Builder builder = Channels.Channel.newBuilder();
        builder.setChannelId(channel.name());
        builder.setContentType(to(channel.contentType()));
        return builder.build();
    }

    public static LzyWhiteboard.Snapshot to(Snapshot snapshot) {
        return LzyWhiteboard.Snapshot.newBuilder().setSnapshotId(snapshot.id().toString()).build();
    }

    public static Lzy.GetS3CredentialsResponse to(StorageCredentials credentials) {
        switch (credentials.type()) {
            case Azure:
                return to((StorageCredentials.AzureCredentials) credentials);
            case AzureSas:
                return to((StorageCredentials.AzureSASCredentials) credentials);
            case Amazon:
                return to((StorageCredentials.AmazonCredentials) credentials);
            default:
            case Empty:
                return GetS3CredentialsResponse.newBuilder().build();
        }
    }

    public static Lzy.GetS3CredentialsResponse to(StorageCredentials.AzureCredentials credentials) {
        return GetS3CredentialsResponse.newBuilder()
            .setAzure(
                AzureCredentials.newBuilder()
                    .setConnectionString(credentials.connectionString())
                    .build()
            )
            .build();
    }

    public static Lzy.GetS3CredentialsResponse to(
        StorageCredentials.AmazonCredentials credentials) {
        return GetS3CredentialsResponse.newBuilder()
            .setAmazon(AmazonCredentials.newBuilder()
                .setEndpoint(credentials.endpoint())
                .setAccessToken(credentials.accessToken())
                .setSecretToken(credentials.secretToken())
                .build())
            .build();
    }

    public static Lzy.GetS3CredentialsResponse to(
        StorageCredentials.AzureSASCredentials credentials) {
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
        WhiteboardField field,
        List<WhiteboardField> dependent,
        @Nullable SnapshotEntryStatus entryStatus) {
        final Builder builder = LzyWhiteboard.WhiteboardField.newBuilder()
            .setFieldName(field.name())
            .addAllDependentFieldNames(
                dependent.stream().map(WhiteboardField::name).collect(Collectors.toList()));
        if (entryStatus == null) {
            builder.setEmpty(true);
            builder.setStatus(Status.CREATED);
        } else {
            builder.setEmpty(entryStatus.empty());
            final URI storage = entryStatus.storage();
            if (storage != null) {
                builder.setStorageUri(storage.toString());
            }
            switch (entryStatus.status()) {
                case CREATED:
                    builder.setStatus(Status.CREATED);
                    break;
                case IN_PROGRESS:
                    builder.setStatus(Status.IN_PROGRESS);
                    break;
                case FINISHED:
                    builder.setStatus(Status.FINISHED);
                    break;
                case ERRORED:
                    builder.setStatus(Status.ERRORED);
                    break;
                default:
                    builder.setStatus(Status.UNKNOWN);
                    break;
            }
        }
        return builder.build();
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

    public static LzyWhiteboard.ExecutionDescription to(SnapshotExecution execution) {
        return LzyWhiteboard.ExecutionDescription.newBuilder()
            .setName(execution.name())
            .setSnapshotId(execution.snapshotId())
            .addAllInput(execution.inputArgs().map(
                arg -> LzyWhiteboard.InputArgDescription.newBuilder()
                    .setEntryId(arg.entryId())
                    .setName(arg.name())
                    .setHash(arg.hash())
                    .build()
            ).collect(Collectors.toList()))
            .addAllOutput(execution.outputArgs().map(
                arg -> LzyWhiteboard.OutputArgDescription.newBuilder()
                    .setEntryId(arg.entryId())
                    .setName(arg.name())
                    .build()
            ).collect(Collectors.toList()))
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
            /* TODO (lindvv):
                    Why do we interact with ZygoteAdapter
                    and construct Env (and other stuff) inside getter
                    instead of creating new class ZygoteImpl
                    and constructing Env inside ZygoteImpl constructor?
             */
            return from(operation.getEnv());
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
            return !slotStatus.getTaskId().isEmpty() ? UUID.fromString(slotStatus.getTaskId())
                : null;
        }

        @Override
        public Slot slot() {
            return from(slotStatus.getDeclaration());
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

    private static class EnvImpl implements Env {
        private final BaseEnv baseEnv;
        private final AuxEnv auxEnv;

        public EnvImpl(BaseEnv baseEnv, AuxEnv auxEnv) {
            this.baseEnv = baseEnv;
            this.auxEnv = auxEnv;
        }

        @Override
        public BaseEnv baseEnv() {
            return baseEnv;
        }

        @Override
        public AuxEnv auxEnv() {
            return auxEnv;
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

    private static class BaseEnvAdapter implements BaseEnv {
        private final Operations.BaseEnv env;

        public BaseEnvAdapter(Operations.BaseEnv env) {
            this.env = env;
        }

        @Override
        public String name() {
            if (env.getName().equals("")) {
                return null;
            }
            return env.getName();
        }
    }
}
