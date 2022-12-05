package ai.lzy.portal.services;

import ai.lzy.fs.fs.LzySlot;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.config.PortalConfig;
import ai.lzy.portal.exceptions.CreateSlotException;
import ai.lzy.portal.exceptions.SnapshotNotFound;
import ai.lzy.portal.exceptions.SnapshotUniquenessException;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortal.PortalSlotDesc;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalApi.*;
import ai.lzy.v1.portal.LzyPortalGrpc.LzyPortalImplBase;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static ai.lzy.portal.grpc.ProtoConverter.buildInputSlotStatus;
import static ai.lzy.portal.grpc.ProtoConverter.buildOutputSlotStatus;

@Singleton
public class PortalService extends LzyPortalImplBase {
    private static final Logger LOG = LogManager.getLogger(PortalService.class);

    public static final String APP = "LzyPortal";
    public static final String PORTAL_SLOT_PREFIX = "/portal_slot";
    public static final String PORTAL_OUT_SLOT_NAME = "/portal_slot:stdout";
    public static final String PORTAL_ERR_SLOT_NAME = "/portal_slot:stderr";

    private final String portalId;
    private final PortalConfig config;

    private final LocalOperationService operationService;
    private final PortalSlotsService slotsService;

    private final AtomicBoolean finished = new AtomicBoolean(false);

    public PortalService(PortalConfig config,
                         @Named("PortalSlotsService") PortalSlotsService slotsService,
                         @Named("PortalOperationsService") LocalOperationService operationService)
    {
        this.portalId = config.getPortalId();
        this.config = config;
        this.operationService = operationService;
        this.slotsService = slotsService;
    }

    @Override
    public void stop(Empty request, StreamObserver<Empty> responseObserver) {
        if (finished.compareAndSet(false, true)) {
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription("Already stopped")
                .asRuntimeException());
        }
    }

    @Override
    public synchronized void status(PortalStatusRequest request,
                                    StreamObserver<PortalStatusResponse> responseObserver)
    {
        var response = LzyPortalApi.PortalStatusResponse.newBuilder();

        var slotNames = new HashSet<>(request.getSlotNamesList());
        var snapshots = slotsService.getSnapshots();

        snapshots.getInputSlots().stream()
            .filter(s -> {
                if (slotNames.isEmpty()) {
                    return true;
                }
                return slotNames.contains(s.name());
            })
            .forEach(slot -> response.addSlots(buildInputSlotStatus(slot)));

        snapshots.getOutputSlots().stream()
            .filter(s -> {
                if (slotNames.isEmpty()) {
                    return true;
                }
                return slotNames.contains(s.name());
            })
            .forEach(slot -> response.addSlots(buildOutputSlotStatus(slot)));

        for (var stdSlot : slotsService.getOutErrSlots()) {
            response.addSlots(buildOutputSlotStatus(stdSlot));
            stdSlot.forEachSlot(slot -> response.addSlots(buildInputSlotStatus(slot)));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public synchronized void openSlots(OpenSlotsRequest request, StreamObserver<OpenSlotsResponse> response) {
        final BiConsumer<String, Status> replyError = (message, status) -> {
            LOG.error(message);
            response.onError(status.withDescription(message).asRuntimeException());
        };

        for (LzyPortal.PortalSlotDesc slotDesc : request.getSlotsList()) {
            LOG.info("Open slot {}", portalSlotToSafeString(slotDesc));

            final Slot slot = ProtoConverter.fromProto(slotDesc.getSlot());
            if (Slot.STDIN.equals(slot) || Slot.ARGS.equals(slot)
                || Slot.STDOUT.equals(slot) || Slot.STDERR.equals(slot))
            {
                replyError.accept("Invalid slot " + slot, Status.INTERNAL);
                return;
            }

            final String taskId = switch (slotDesc.getKindCase()) {
                case SNAPSHOT -> portalId;
                case STDERR -> slotDesc.getStderr().getTaskId();
                case STDOUT -> slotDesc.getStdout().getTaskId();
                default -> throw new NotImplementedException(slotDesc.getKindCase().name());
            };
            final SlotInstance slotInstance = new SlotInstance(slot, taskId, slotDesc.getChannelId(),
                slotsService.getSlotsManager().resolveSlotUri(taskId, slot.name()));

            try {
                LzySlot newLzySlot = switch (slotDesc.getKindCase()) {
                    case SNAPSHOT -> slotsService.getSnapshots().createSlot(slotDesc.getSnapshot(), slotInstance);
                    case STDOUT -> slotsService.getStdoutSlot().attach(slotInstance);
                    case STDERR -> slotsService.getStderrSlot().attach(slotInstance);
                    default -> throw new NotImplementedException(slotDesc.getKindCase().name());
                };
                slotsService.getSlotsManager().registerSlot(newLzySlot);
            } catch (SnapshotNotFound e) {
                replyError.accept(e.getMessage(), Status.NOT_FOUND);
            } catch (SnapshotUniquenessException e) {
                replyError.accept(e.getMessage(), Status.INVALID_ARGUMENT);
            } catch (CreateSlotException e) {
                replyError.accept(e.getMessage(), Status.INTERNAL);
            }
        }

        response.onNext(OpenSlotsResponse.newBuilder().setSuccess(true).build());
        response.onCompleted();
    }

    @Override
    public void finish(FinishRequest request, StreamObserver<FinishResponse> responseObserver) {
        LOG.info("Finishing portal with id <{}>", portalId);
        if (!finished.compareAndSet(false, true)) {
            throw new IllegalStateException("Cannot finish already finished portal");
        }

        for (var slot: slotsService.getSnapshots().getOutputSlots()) {
            try {
                slot.close();
            } catch (Exception e) {
                LOG.error("Cannot close slot <{}>:", slot.name(), e);
            }
        }

        for (var slot: slotsService.getSnapshots().getInputSlots()) {
            try {
                slot.close();
            } catch (Exception e) {
                LOG.error("Cannot close slot <{}>:", slot.name(), e);
            }
        }

        try {
            slotsService.getStdoutSlot().finish();
        } catch (Exception e) {
            LOG.error("Cannot finish stdout slot in portal with id <{}>: ", portalId, e);
        }
        try {
            slotsService.getStderrSlot().finish();
        } catch (Exception e) {
            LOG.error("Cannot finish stderr slot in portal with id <{}>: ", portalId, e);
        }

        responseObserver.onNext(FinishResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    private static String portalSlotToSafeString(PortalSlotDesc slotDesc) {
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
