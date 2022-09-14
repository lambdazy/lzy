package ai.lzy.model.deprecated;

import static ai.lzy.model.grpc.ProtoConverter.fromProto;

import ai.lzy.model.StorageCredentials;
import ai.lzy.model.data.DataSchema;
import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.model.slot.SlotStatus;
import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.channel.LCMS;
import ai.lzy.v1.common.LMD;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.deprecated.Lzy;
import ai.lzy.v1.deprecated.LzyTask;
import ai.lzy.v1.deprecated.LzyZygote;
import java.net.URI;
import java.util.stream.Stream;
import javax.annotation.Nullable;

@Deprecated
public abstract class GrpcConverter {

    public static AtomicZygote from(LzyZygote.Zygote grpcOperation) {
        return new AtomicZygoteAdapter(grpcOperation);
    }

    public static SlotInstance from(LMS.SlotInstance slotInstance) {
        return new SlotInstance(
            fromProto(slotInstance.getSlot()),
            slotInstance.getTaskId(),
            slotInstance.getChannelId(),
            URI.create(slotInstance.getSlotUri())
        );
    }
    public static Context from(LzyTask.ContextSpec spec) {
        return new ContextImpl(
            fromProto(spec.getEnv()),
            fromProto(spec.getProvisioning()),
            from(spec.getAssignmentsList().stream())
        );
    }

    public static Stream<Context.SlotAssignment> from(Stream<LzyTask.SlotAssignment> assignmentsList) {
        return assignmentsList
            .map(ass -> new Context.SlotAssignment(ass.getTaskId(), fromProto(ass.getSlot()), ass.getBinding()));
    }

    public static DataSchema contentTypeFrom(LMD.DataScheme dataScheme) {
        return DataSchema.buildDataSchema(dataScheme.getSchemeType(), dataScheme.getType());
    }

    public static LCMS.ChannelCreateRequest createChannelRequest(String workflowId, LCM.ChannelSpec spec) {
        return LCMS.ChannelCreateRequest.newBuilder()
            .setWorkflowId(workflowId)
            .setChannelSpec(spec)
            .build();
    }

    public static LzyZygote.Zygote to(Zygote zygote) {
        final LzyZygote.Zygote.Builder builder = LzyZygote.Zygote.newBuilder();
        if (zygote instanceof AtomicZygote) {
            final AtomicZygote atomicZygote = (AtomicZygote) zygote;
            builder.setEnv(ProtoConverter.toProto(atomicZygote.env()));
            builder.setName(atomicZygote.name());
            builder.setProvisioning(ProtoConverter.toProto(atomicZygote.provisioning()));
            builder.setFuze(atomicZygote.fuze());
        }
        Stream.concat(Stream.of(zygote.input()), Stream.of(zygote.output()))
            .forEach(slot -> builder.addSlots(ProtoConverter.toProto(slot)));
        return builder.build();
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
                return Lzy.GetS3CredentialsResponse.newBuilder().build();
        }
    }

    public static Lzy.GetS3CredentialsResponse to(StorageCredentials.AzureCredentials credentials) {
        return Lzy.GetS3CredentialsResponse.newBuilder()
            .setAzure(
                Lzy.AzureCredentials.newBuilder()
                    .setConnectionString(credentials.connectionString())
                    .build()
            )
            .build();
    }

    public static Lzy.GetS3CredentialsResponse to(
        StorageCredentials.AmazonCredentials credentials) {
        return Lzy.GetS3CredentialsResponse.newBuilder()
            .setAmazon(Lzy.AmazonCredentials.newBuilder()
                .setEndpoint(credentials.endpoint())
                .setAccessToken(credentials.accessToken())
                .setSecretToken(credentials.secretToken())
                .build())
            .build();
    }

    public static Lzy.GetS3CredentialsResponse to(
        StorageCredentials.AzureSASCredentials credentials) {
        return Lzy.GetS3CredentialsResponse.newBuilder()
            .setAzureSas(
                Lzy.AzureSASCredentials.newBuilder()
                    .setSignature(credentials.signature())
                    .setEndpoint(credentials.endpoint())
                    .build()
            )
            .build();
    }

    public static LMS.SlotInstance to(SlotInstance slotInstance) {
        return LMS.SlotInstance.newBuilder()
            .setSlot(ProtoConverter.toProto(slotInstance.spec()))
            .setTaskId(slotInstance.taskId())
            .setChannelId(slotInstance.channelId())
            .setSlotUri(slotInstance.uri().toString())
            .build();
    }

    private record AtomicZygoteAdapter(LzyZygote.Zygote operation) implements AtomicZygote {

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
                .filter(s -> s.getDirection() == LMS.Slot.Direction.INPUT)
                .map(SlotAdapter::new)
                .toArray(Slot[]::new);
        }

        @Override
        public Slot[] output() {
            return operation.getSlotsList().stream()
                .filter(s -> s.getDirection() == LMS.Slot.Direction.OUTPUT)
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
            return fromProto(operation.getEnv());
        }

        @Override
        public Provisioning provisioning() {
            return fromProto(operation.getProvisioning());
        }

        @Override
        public LzyZygote.Zygote zygote() {
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

        private final LMS.Slot s;

        SlotAdapter(LMS.Slot s) {
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
            return SlotStatus.State.valueOf(slotStatus.getState().name());
        }
    }

}
