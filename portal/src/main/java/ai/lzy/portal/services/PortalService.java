package ai.lzy.portal.services;

import ai.lzy.fs.fs.LzySlot;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.LocalOperationService.ImmutableCopyOperation;
import ai.lzy.longrunning.Operation;
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
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOpResult;
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
        finished.set(true);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public synchronized void status(PortalStatusRequest request,
                                    StreamObserver<PortalStatusResponse> responseObserver)
    {
        if (assertActive(responseObserver)) {
            return;
        }

        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOpResult(operationService, idempotencyKey, PortalStatusResponse.class,
            responseObserver, "Cannot obtain status of portal slots", LOG))
        {
            return;
        }

        var op = Operation.create(portalId, "Slots status", idempotencyKey, null);
        ImmutableCopyOperation opSnapshot = operationService.registerOperation(op);

        if (op.id().equals(opSnapshot.id())) {
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

            operationService.updateResponse(op.id(), response.build());
        } else {
            LOG.info("Found operation by idempotency key: {}", opSnapshot.toString());
        }

        var typeResp = PortalStatusResponse.class;
        var internalErrorMessage = "Cannot obtain status of portal slots";

        var opId = opSnapshot.id();

        opSnapshot = operationService.awaitOperationCompletion(opId, Duration.ofMillis(50), Duration.ofSeconds(5));

        if (opSnapshot == null) {
            LOG.error("Can not find operation with id: { opId: {} }", opId);
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
            return;
        }

        if (opSnapshot.done()) {
            if (opSnapshot.response() != null) {
                try {
                    var resp = opSnapshot.response().unpack(typeResp);
                    responseObserver.onNext(resp);
                    responseObserver.onCompleted();
                } catch (InvalidProtocolBufferException e) {
                    LOG.error("Error while waiting op result: {}", e.getMessage(), e);
                    responseObserver.onError(Status.INTERNAL.asRuntimeException());
                }
            } else {
                var error = opSnapshot.error();
                assert error != null;
                responseObserver.onError(error.asRuntimeException());
            }
        } else {
            LOG.error("Waiting deadline exceeded, operation: {}", opSnapshot.toShortString());
            responseObserver.onError(Status.INTERNAL.withDescription(internalErrorMessage).asRuntimeException());
        }
    }

    @Override
    public synchronized void openSlots(OpenSlotsRequest request, StreamObserver<OpenSlotsResponse> response) {
        if (assertActive(response)) {
            return;
        }

        final BiConsumer<String, Status> replyError = (message, status) -> {
            LOG.error(message);
            response.onError(status.withDescription(message).asRuntimeException());
        };

        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOpResult(operationService, idempotencyKey, OpenSlotsResponse.class,
            response, "Cannot open slots", LOG))
        {
            return;
        }

        var op = Operation.create(portalId, "Open slots", idempotencyKey, Any.pack(Empty.getDefaultInstance()));
        var opSnapshot = operationService.registerOperation(op);

        if (op.id().equals(opSnapshot.id())) {
            for (LzyPortal.PortalSlotDesc slotDesc : request.getSlotsList()) {
                LOG.info("Open slot {}", portalSlotToSafeString(slotDesc));

                final Slot slot = ProtoConverter.fromProto(slotDesc.getSlot());
                if (Slot.STDIN.equals(slot) || Slot.ARGS.equals(slot)
                    || Slot.STDOUT.equals(slot) || Slot.STDERR.equals(slot))
                {
                    operationService.updateError(op.id(), Status.INTERNAL.withDescription("Invalid slot " + slot));
                    replyError.accept("Invalid slot " + slot, Status.INTERNAL);
                    return;
                }

                try {
                    final String taskId = switch (slotDesc.getKindCase()) {
                        case SNAPSHOT -> portalId;
                        case STDERR -> slotDesc.getStderr().getTaskId();
                        case STDOUT -> slotDesc.getStdout().getTaskId();
                        default -> throw new NotImplementedException(slotDesc.getKindCase().name());
                    };
                    final SlotInstance slotInstance = new SlotInstance(slot, taskId, slotDesc.getChannelId(),
                        slotsService.getSlotsManager().resolveSlotUri(taskId, slot.name()));

                    LzySlot newLzySlot = switch (slotDesc.getKindCase()) {
                        case SNAPSHOT -> slotsService.getSnapshots().createSlot(slotDesc.getSnapshot(), slotInstance);
                        case STDOUT -> slotsService.getStdoutSlot().attach(slotInstance);
                        case STDERR -> slotsService.getStderrSlot().attach(slotInstance);
                        default -> throw new NotImplementedException(slotDesc.getKindCase().name());
                    };
                    slotsService.getSlotsManager().registerSlot(newLzySlot);
                } catch (SnapshotNotFound e) {
                    operationService.updateError(op.id(), Status.NOT_FOUND.withDescription(e.getMessage()));
                    replyError.accept(e.getMessage(), Status.NOT_FOUND);
                    return;
                } catch (SnapshotUniquenessException | NotImplementedException e) {
                    operationService.updateError(op.id(), Status.INVALID_ARGUMENT.withDescription(e.getMessage()));
                    replyError.accept(e.getMessage(), Status.INVALID_ARGUMENT);
                    return;
                } catch (CreateSlotException e) {
                    operationService.updateError(op.id(), Status.INTERNAL.withDescription(e.getMessage()));
                    replyError.accept(e.getMessage(), Status.INTERNAL);
                    return;
                }
            }

            operationService.updateResponse(op.id(), OpenSlotsResponse.newBuilder().setSuccess(true).build());
        } else {
            LOG.info("Found operation by idempotency key: {}", opSnapshot.toString());
        }

        var typeResp = OpenSlotsResponse.class;
        var internalErrorMessage = "Cannot open slot";

        var opId = opSnapshot.id();

        opSnapshot = operationService.awaitOperationCompletion(opId, Duration.ofMillis(50), Duration.ofSeconds(5));

        if (opSnapshot == null) {
            LOG.error("Can not find operation with id: { opId: {} }", opId);
            response.onError(Status.INTERNAL.asRuntimeException());
            return;
        }

        if (opSnapshot.done()) {
            if (opSnapshot.response() != null) {
                try {
                    var resp = opSnapshot.response().unpack(typeResp);
                    response.onNext(resp);
                    response.onCompleted();
                } catch (InvalidProtocolBufferException e) {
                    LOG.error("Error while waiting op result: {}", e.getMessage(), e);
                    response.onError(Status.INTERNAL.asRuntimeException());
                }
            } else {
                var error = opSnapshot.error();
                assert error != null;
                response.onError(error.asRuntimeException());
            }
        } else {
            LOG.error("Waiting deadline exceeded, operation: {}", opSnapshot.toShortString());
            response.onError(Status.INTERNAL.withDescription(internalErrorMessage).asRuntimeException());
        }
    }

    @Override
    public void finish(FinishRequest request, StreamObserver<FinishResponse> responseObserver) {
        if (assertActive(responseObserver)) {
            return;
        }

        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOpResult(operationService, idempotencyKey, FinishResponse.class,
            responseObserver, "Cannot finish portal", LOG))
        {
            return;
        }

        var op = Operation.create(portalId, "Finish portal", idempotencyKey, null);
        var opSnapshot = operationService.registerOperation(op);

        if (op.id().equals(opSnapshot.id())) {
            finished.set(true);

            LOG.info("Finishing portal with id <{}>", portalId);

            for (var slot : slotsService.getSnapshots().getOutputSlots()) {
                try {
                    slot.close();
                } catch (Exception e) {
                    LOG.error("Cannot close slot <{}>:", slot.name(), e);
                }
            }

            for (var slot : slotsService.getSnapshots().getInputSlots()) {
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

            operationService.updateResponse(op.id(), FinishResponse.getDefaultInstance());
        } else {
            LOG.info("Found operation by idempotency key: {}", opSnapshot.toString());
        }

        var typeResp = FinishResponse.class;
        var internalErrorMessage = "Cannot finish portal";

        var opId = opSnapshot.id();

        opSnapshot = operationService.awaitOperationCompletion(opId, Duration.ofMillis(50), Duration.ofSeconds(5));

        if (opSnapshot == null) {
            LOG.error("Can not find operation with id: { opId: {} }", opId);
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
            return;
        }

        if (opSnapshot.done()) {
            if (opSnapshot.response() != null) {
                try {
                    var resp = opSnapshot.response().unpack(typeResp);
                    responseObserver.onNext(resp);
                    responseObserver.onCompleted();
                } catch (InvalidProtocolBufferException e) {
                    LOG.error("Error while waiting op result: {}", e.getMessage(), e);
                    responseObserver.onError(Status.INTERNAL.asRuntimeException());
                }
            } else {
                var error = opSnapshot.error();
                assert error != null;
                responseObserver.onError(error.asRuntimeException());
            }
        } else {
            LOG.error("Waiting deadline exceeded, operation: {}", opSnapshot.toShortString());
            responseObserver.onError(Status.INTERNAL.withDescription(internalErrorMessage).asRuntimeException());
        }
    }

    private <T extends Message> boolean assertActive(StreamObserver<T> response) {
        if (finished.get()) {
            response.onError(Status.FAILED_PRECONDITION.withDescription("Portal already stopped").asRuntimeException());
            return true;
        }
        return false;
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
