package ai.lzy.model;

import ai.lzy.model.channel.ChannelSpec;
import ai.lzy.model.data.DataSchema;
import ai.lzy.model.data.types.SchemeType;
import ai.lzy.model.graph.AtomicZygote;
import ai.lzy.model.graph.AuxEnv;
import ai.lzy.model.graph.BaseEnv;
import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.LocalModule;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.model.graph.PythonEnv;
import ai.lzy.model.snapshot.ExecutionSnapshot;
import ai.lzy.model.snapshot.ExecutionValue;
import ai.lzy.model.snapshot.InputExecutionValue;
import ai.lzy.model.snapshot.Snapshot;
import ai.lzy.model.snapshot.SnapshotEntry;
import ai.lzy.model.snapshot.SnapshotEntryStatus;
import ai.lzy.model.snapshot.WhiteboardField;
import ai.lzy.model.snapshot.WhiteboardStatus;
import ai.lzy.v1.Channels;
import ai.lzy.v1.Lzy;
import ai.lzy.v1.Lzy.AmazonCredentials;
import ai.lzy.v1.Lzy.AzureCredentials;
import ai.lzy.v1.Lzy.AzureSASCredentials;
import ai.lzy.v1.Lzy.GetS3CredentialsResponse;
import ai.lzy.v1.LzyFsApi;
import ai.lzy.v1.LzyWhiteboard;
import ai.lzy.v1.LzyWhiteboard.WhiteboardField.Builder;
import ai.lzy.v1.LzyWhiteboard.WhiteboardField.Status;
import ai.lzy.v1.Operations;
import ai.lzy.v1.Operations.Provisioning.Tag;
import ai.lzy.v1.Tasks.ContextSpec;
import ai.lzy.v1.Tasks.SlotAssignment;
import com.google.protobuf.Timestamp;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public abstract class GrpcConverter {

    public static AtomicZygote from(Operations.Zygote grpcOperation) {
        return new AtomicZygoteAdapter(grpcOperation);
    }

    public static Slot from(Operations.Slot grpcSlot) {
        return new SlotAdapter(grpcSlot);
    }

    public static SlotStatus from(Operations.SlotStatus slotStatus) {
        return new SlotStatusAdapter(slotStatus);
    }

    public static Provisioning from(Operations.Provisioning provisioning) {
        return () -> provisioning.getTagsList().stream().map(Tag::getTag).collect(Collectors.toSet());
    }

    public static Env from(Operations.EnvSpec env) {
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

    public static Stream<Context.SlotAssignment> from(Stream<SlotAssignment> assignmentsList) {
        return assignmentsList
            .map(ass -> new Context.SlotAssignment(ass.getTaskId(), from(ass.getSlot()), ass.getBinding()));
    }

    public static Date from(Timestamp date) {
        return Date.from(Instant.ofEpochSecond(date.getSeconds(), date.getNanos()));
    }

    public static ExecutionSnapshot from(LzyWhiteboard.ExecutionDescription description) {
        ArrayList<ExecutionValue> outputs = new ArrayList<>();
        ArrayList<InputExecutionValue> inputs = new ArrayList<>();
        ExecutionSnapshot execution = new ExecutionSnapshot.ExecutionSnapshotImpl(
            description.getName(),
            description.getSnapshotId(),
            outputs,
            inputs
        );
        description.getOutputList().stream().map(arg ->
            new ExecutionSnapshot.ExecutionValueImpl(arg.getName(), execution.snapshotId(), arg.getEntryId())
        ).forEach(outputs::add);
        description.getInputList().stream().map(arg ->
            new ExecutionSnapshot.InputExecutionValueImpl(arg.getName(), execution.snapshotId(),
                arg.getEntryId(), arg.getHash())
        ).forEach(inputs::add);
        return execution;
    }

    public static SlotInstance from(LzyFsApi.SlotInstance slotInstance) {
        return new SlotInstance(
            from(slotInstance.getSlot()),
            slotInstance.getTaskId(),
            slotInstance.getChannelId(),
            URI.create(slotInstance.getSlotUri())
        );
    }

    public static DataSchema contentTypeFrom(Operations.DataScheme dataScheme) {
        return DataSchema.buildDataSchema(dataScheme.getSchemeType().name(), dataScheme.getType());
    }

    public static Operations.Zygote to(Zygote zygote) {
        final Operations.Zygote.Builder builder = Operations.Zygote.newBuilder();
        if (zygote instanceof AtomicZygote) {
            final AtomicZygote atomicZygote = (AtomicZygote) zygote;
            builder.setEnv(to(atomicZygote.env()));
            builder.setName(atomicZygote.name());
            builder.setProvisioning(to(atomicZygote.provisioning()));
            builder.setFuze(atomicZygote.fuze());
        }
        Stream.concat(Stream.of(zygote.input()), Stream.of(zygote.output()))
            .forEach(slot -> builder.addSlots(to(slot)));
        return builder.build();
    }

    public static Operations.EnvSpec to(Env env) {
        Operations.EnvSpec.Builder builder = Operations.EnvSpec.newBuilder();
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

    public static Operations.DataScheme to(DataSchema dataSchema) {
        return Operations.DataScheme.newBuilder()
            .setType(dataSchema.typeContent())
            .setSchemeType(to(dataSchema.schemeType()))
            .build();
    }

    private static Operations.Provisioning to(Provisioning provisioning) {
        return Operations.Provisioning.newBuilder()
            .addAllTags(provisioning.tags().stream()
                .map(tag -> Tag.newBuilder().setTag(tag).build())
                .collect(Collectors.toList()))
            .build();
    }


    public static Channels.ChannelSpec to(ChannelSpec channel) {
        final Channels.ChannelSpec.Builder builder = Channels.ChannelSpec.newBuilder();
        builder.setChannelName(channel.name());
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

            DataSchema schema = entryStatus.schema();
            if (schema != null) {
                builder.setScheme(GrpcConverter.to(schema));
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
            case ERRORED:
                return LzyWhiteboard.WhiteboardStatus.ERRORED;
            default:
                throw new IllegalArgumentException("Unknown state: " + state);
        }
    }

    public static LzyWhiteboard.ExecutionDescription to(ExecutionSnapshot execution) {
        return LzyWhiteboard.ExecutionDescription.newBuilder()
            .setName(execution.name())
            .setSnapshotId(execution.snapshotId())
            .addAllInput(execution.inputs().map(
                arg -> LzyWhiteboard.InputArgDescription.newBuilder()
                    .setEntryId(arg.entryId())
                    .setName(arg.name())
                    .setHash(arg.hash())
                    .build()
            ).collect(Collectors.toList()))
            .addAllOutput(execution.outputs().map(
                arg -> LzyWhiteboard.OutputArgDescription.newBuilder()
                    .setEntryId(arg.entryId())
                    .setName(arg.name())
                    .build()
            ).collect(Collectors.toList()))
            .build();
    }

    public static Operations.SchemeType to(SchemeType dataSchema) {
        return Operations.SchemeType.valueOf(dataSchema.name());
    }

    public static LzyFsApi.SlotInstance to(SlotInstance slotInstance) {
        return LzyFsApi.SlotInstance.newBuilder()
            .setSlot(to(slotInstance.spec()))
            .setTaskId(slotInstance.taskId())
            .setChannelId(slotInstance.channelId())
            .setSlotUri(slotInstance.uri().toString())
            .build();
    }

    private record AtomicZygoteAdapter(Operations.Zygote operation) implements AtomicZygote {

        @Override
        public String name() {
            return operation.getName();
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
        public String tid() {
            slotStatus.getTaskId();
            return !slotStatus.getTaskId().isEmpty() ? slotStatus.getTaskId() : null;
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

        @Nullable
        @Override
        public StorageCredentials credentials() {
            // TODO(artolord) add credentials to proto
            return null;
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