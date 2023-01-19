package ai.lzy.portal.services;

import ai.lzy.fs.fs.LzySlot;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.LocalOperationUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.App;
import ai.lzy.portal.config.PortalConfig;
import ai.lzy.portal.exceptions.CreateSlotException;
import ai.lzy.portal.exceptions.SnapshotNotFound;
import ai.lzy.portal.exceptions.SnapshotUniquenessException;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortal.PortalSlotDesc;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalApi.*;
import ai.lzy.v1.portal.LzyPortalGrpc.LzyPortalImplBase;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
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

    private final ApplicationContext context;

    private final String portalId;

    private final LocalOperationService operationService;
    private final PortalSlotsService slotsService;

    private final AtomicBoolean finished = new AtomicBoolean(false);

    public PortalService(ApplicationContext context, PortalConfig config,
                         @Named("PortalSlotsService") PortalSlotsService slotsService,
                         @Named("PortalOperationsService") LocalOperationService operationService)
    {
        this.context = context;

        this.portalId = config.getPortalId();
        this.operationService = operationService;
        this.slotsService = slotsService;
    }

    @Override
    public void stop(Empty request, StreamObserver<Empty> responseObserver) {
        LOG.info("Stop portal with id <{}>", portalId);

        finished.set(true);

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
            slotsService.getStdoutSlot().close();
        } catch (Exception e) {
            LOG.error("Cannot finish stdout slot in portal with id <{}>: ", portalId, e);
        }

        try {
            slotsService.getStderrSlot().close();
        } catch (Exception e) {
            LOG.error("Cannot finish stderr slot in portal with id <{}>: ", portalId, e);
        }

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();

        var main = context.getBean(App.class);
        main.stop();
    }

    @Override
    public synchronized void status(PortalStatusRequest request,
                                    StreamObserver<PortalStatusResponse> response)
    {
        if (assertActive(response)) {
            return;
        }

        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        var errorMsg = "Cannot obtain status of portal slots";
        if (idempotencyKey != null &&
            loadExistingOpResult(idempotencyKey, PortalStatusResponse.class, response, errorMsg))
        {
            return;
        }

        var op = Operation.create(portalId, "Slots status", null, idempotencyKey, null);
        var opSnapshot = operationService.registerOperation(op);

        if (op.id().equals(opSnapshot.id())) {
            var resp = LzyPortalApi.PortalStatusResponse.newBuilder();

            var slotNames = new HashSet<>(request.getSlotNamesList());
            var snapshots = slotsService.getSnapshots();

            snapshots.getInputSlots().stream()
                .filter(s -> {
                    if (slotNames.isEmpty()) {
                        return true;
                    }
                    return slotNames.contains(s.name());
                })
                .forEach(slot -> resp.addSlots(buildInputSlotStatus(slot)));

            snapshots.getOutputSlots().stream()
                .filter(s -> {
                    if (slotNames.isEmpty()) {
                        return true;
                    }
                    return slotNames.contains(s.name());
                })
                .forEach(slot -> resp.addSlots(buildOutputSlotStatus(slot)));

            for (var stdSlot : slotsService.getOutErrSlots()) {
                resp.addSlots(buildOutputSlotStatus(stdSlot));
                stdSlot.forEachSlot(slot -> resp.addSlots(buildInputSlotStatus(slot)));
            }

            operationService.updateResponse(op.id(), resp.build());
            response.onNext(resp.build());
            response.onCompleted();
        } else {
            LOG.info("Found operation by idempotency key: {}", opSnapshot.toString());

            awaitOpAndReply(opSnapshot.id(), PortalStatusResponse.class, response, errorMsg);
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
        if (idempotencyKey != null &&
            loadExistingOpResult(idempotencyKey, OpenSlotsResponse.class, response, "Cannot open slots"))
        {
            return;
        }

        var op = Operation.create(portalId, "Open slots", null, idempotencyKey, Any.pack(Empty.getDefaultInstance()));
        var opSnapshot = operationService.registerOperation(op);
        var snapshotId = opSnapshot.id();

        if (op.id().equals(snapshotId)) {
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
                    e.printStackTrace();
                    operationService.updateError(op.id(), Status.NOT_FOUND.withDescription(e.getMessage()));
                    replyError.accept(e.getMessage(), Status.NOT_FOUND);
                    return;
                } catch (SnapshotUniquenessException | NotImplementedException e) {
                    e.printStackTrace();
                    operationService.updateError(op.id(), Status.INVALID_ARGUMENT.withDescription(e.getMessage()));
                    replyError.accept(e.getMessage(), Status.INVALID_ARGUMENT);
                    return;
                } catch (CreateSlotException e) {
                    e.printStackTrace();
                    operationService.updateError(op.id(), Status.INTERNAL.withDescription(e.getMessage()));
                    replyError.accept(e.getMessage(), Status.INTERNAL);
                    return;
                }
            }

            var resp = OpenSlotsResponse.newBuilder().setSuccess(true).build();

            operationService.updateResponse(op.id(), resp);
            response.onNext(resp);
            response.onCompleted();
        } else {
            LOG.info("Found operation by idempotency key: {}", opSnapshot.toString());

            awaitOpAndReply(snapshotId, OpenSlotsResponse.class, response, "Cannot open slot");
        }
    }

    @Override
    public void finish(FinishRequest request, StreamObserver<LongRunning.Operation> response) {
        if (assertActive(response)) {
            return;
        }

        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(idempotencyKey, response)) {
            return;
        }

        var op = Operation.create(portalId, "Finish portal", null, idempotencyKey, null);
        var opSnapshot = operationService.registerOperation(op);

        if (op.id().equals(opSnapshot.id())) {
            response.onNext(opSnapshot.toProto());
            response.onCompleted();

            finished.set(true);

            LOG.info("Finishing portal with id <{}>", portalId);

            String errorMessage = null;

            for (var slot : slotsService.getSnapshots().getOutputSlots()) {
                try {
                    slot.close();
                } catch (Exception e) {
                    if (errorMessage == null) {
                        errorMessage = "Cannot close slot '" + slot.name() + "'";
                    }
                    LOG.error("Cannot close slot <{}>:", slot.name(), e);
                }
            }

            for (var slot : slotsService.getSnapshots().getInputSlots()) {
                try {
                    slot.close();
                } catch (Exception e) {
                    if (errorMessage == null) {
                        errorMessage = "Cannot close slot '" + slot.name() + "'";
                    }
                    LOG.error("Cannot close slot <{}>:", slot.name(), e);
                }
            }

            try {
                slotsService.getStdoutSlot().finish();
            } catch (Exception e) {
                if (errorMessage == null) {
                    errorMessage = "Cannot finish stdout slot in portal";
                }
                LOG.error("Cannot finish stdout slot in portal with id <{}>: ", portalId, e);
            }

            try {
                slotsService.getStderrSlot().finish();
            } catch (Exception e) {
                if (errorMessage == null) {
                    errorMessage = "Cannot finish stderr slot in portal";
                }
                LOG.error("Cannot finish stderr slot in portal with id <{}>: ", portalId, e);
            }

            try {
                slotsService.getStdoutSlot().await();
            } catch (Exception e) {
                if (errorMessage == null) {
                    errorMessage = "Cannot await finish stdout slot in portal";
                }
                LOG.error("Cannot await finish stdout slot in portal with id <{}>: ", portalId, e);
            }

            try {
                slotsService.getStderrSlot().await();
            } catch (Exception e) {
                if (errorMessage == null) {
                    errorMessage = "Cannot await finish stderr slot in portal";
                }
                LOG.error("Cannot await finish stderr slot in portal with id <{}>: ", portalId, e);
            }

            if (errorMessage != null) {
                operationService.updateError(op.id(), Status.INTERNAL.withDescription(errorMessage));
            } else {
                operationService.updateResponse(op.id(), FinishResponse.getDefaultInstance());
            }
        } else {
            LOG.info("Found operation by idempotency key: {}", opSnapshot.toString());
            response.onNext(opSnapshot.toProto());
            response.onCompleted();
        }
    }

    private boolean loadExistingOp(Operation.IdempotencyKey ik, StreamObserver<LongRunning.Operation> response) {
        return IdempotencyUtils.loadExistingOp(operationService, ik, response, LOG);
    }

    private <T extends Message> boolean loadExistingOpResult(Operation.IdempotencyKey key, Class<T> respType,
                                                             StreamObserver<T> response, String errorMsg)
    {
        return IdempotencyUtils.loadExistingOpResult(operationService, key, respType, response, errorMsg, LOG);
    }

    private <T extends Message> void awaitOpAndReply(String opId, Class<T> respType,
                                                     StreamObserver<T> response, String errorMsg)
    {
        LocalOperationUtils.awaitOpAndReply(operationService, opId, response, respType, errorMsg, LOG);
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
            case SNAPSHOT -> sb.append("\"snapshot/uri:").append(slotDesc.getSnapshot().getStorageConfig().getUri())
                .append("\"");
            case STDOUT -> sb.append("\"stdout\"");
            case STDERR -> sb.append("\"stderr\"");
            default -> sb.append("\"").append(slotDesc.getKindCase()).append("\"");
        }

        return sb.append("}").toString();
    }
}
