package ai.lzy.portal;

import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.deprecated.GrpcConverter;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.basic.SlotInstance;
import ai.lzy.portal.utils.PortalUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalApi.OpenSlotsRequest;
import ai.lzy.v1.portal.LzyPortalApi.OpenSlotsResponse;
import ai.lzy.v1.portal.LzyPortalApi.PortalStatus;
import ai.lzy.v1.portal.LzyPortalGrpc.LzyPortalImplBase;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;

class PortalApiImpl extends LzyPortalImplBase {
    private static final Logger LOG = LogManager.getLogger(PortalApiImpl.class);

    private final Portal portal;

    PortalApiImpl(Portal portal) {
        this.portal = portal;
    }

    @Override
    public void stop(Empty request, StreamObserver<Empty> responseObserver) {
        portal.shutdown();
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();

    }

    @Override
    public synchronized void status(Empty request, StreamObserver<PortalStatus> responseObserver) {
        var response = LzyPortalApi.PortalStatus.newBuilder();

        var snapshots = portal.getSnapshots();
        snapshots.getInputSlots().forEach(slot ->
            response.addSlots(PortalUtils.buildInputSlotStatus(slot)));
        snapshots.getOutputSlots().forEach(slot ->
            response.addSlots(PortalUtils.buildOutputSlotStatus(slot)));

        for (var stdSlot : portal.getOutErrSlots()) {
            response.addSlots(PortalUtils.buildOutputSlotStatus(stdSlot));
            stdSlot.forEachSlot(slot -> response.addSlots(PortalUtils.buildInputSlotStatus(slot)));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();

    }

    @Override
    public synchronized void openSlots(OpenSlotsRequest request, StreamObserver<OpenSlotsResponse> responseObserver) {
        try {
            var response = openInternal(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            responseObserver.onError(e);
        }

    }

    private OpenSlotsResponse openInternal(OpenSlotsRequest request) {
        LOG.info("Configure portal slots request.");

        var response = OpenSlotsResponse.newBuilder().setSuccess(true);

        final Function<String, OpenSlotsResponse> replyError = message -> {
            LOG.error(message);
            response.setSuccess(false);
            response.setDescription(message);
            return response.build();
        };

        for (LzyPortal.PortalSlotDesc slotDesc : request.getSlotsList()) {
            LOG.info("Open slot {}", portalSlotToSafeString(slotDesc));

            final Slot slot = ProtoConverter.fromProto(slotDesc.getSlot());
            if (Slot.STDIN.equals(slot) || Slot.ARGS.equals(slot)
                || Slot.STDOUT.equals(slot) || Slot.STDERR.equals(slot)) {
                return replyError.apply("Invalid slot " + slot);
            }

            final String taskId = switch (slotDesc.getKindCase()) {
                case SNAPSHOT -> portal.getPortalTaskId();
                case STDERR -> slotDesc.getStderr().getTaskId();
                case STDOUT -> slotDesc.getStdout().getTaskId();
                default -> throw new NotImplementedException(slotDesc.getKindCase().name());
            };
            final SlotInstance slotInstance = new SlotInstance(slot, taskId, slotDesc.getChannelId(),
                portal.getSlotManager().resolveSlotUri(taskId, slot.name()));

            try {
                LzySlot newLzySlot = switch (slotDesc.getKindCase()) {
                    case SNAPSHOT -> portal.getSnapshots().createSlot(slotDesc.getSnapshot(), slotInstance);
                    case STDOUT -> portal.getStdoutSlot().attach(slotInstance);
                    case STDERR -> portal.getStderrSlot().attach(slotInstance);
                    default -> throw new NotImplementedException(slotDesc.getKindCase().name());
                };
                portal.getSlotManager().registerSlot(newLzySlot);
            } catch (Portal.CreateSlotException e) {
                replyError.apply(e.getMessage());
            }
        }

        return response.build();
    }

    private static String portalSlotToSafeString(LzyPortal.PortalSlotDesc slotDesc) {
        var sb = new StringBuilder()
            .append("PortalSlotDesc{")
            .append("\"slot\": ").append(JsonUtils.printSingleLine(slotDesc))
            .append(", \"storage\": ");

        switch (slotDesc.getKindCase()) {
            case SNAPSHOT -> sb.append("\"snapshot/key:").append(slotDesc.getSnapshot().getS3().getKey())
                .append("/bucket:").append(slotDesc.getSnapshot().getS3().getBucket()).append("\"");
            case STDOUT -> sb.append("\"stdout\"");
            case STDERR -> sb.append("\"stderr\"");
            default -> sb.append("\"").append(slotDesc.getKindCase()).append("\"");
        }

        return sb.append("}").toString();
    }
}
