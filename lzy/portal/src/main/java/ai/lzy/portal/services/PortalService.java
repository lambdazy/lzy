package ai.lzy.portal.services;

import ai.lzy.fs.fs.LzySlot;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.LocalOperationServiceUtils;
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
import ai.lzy.util.grpc.ProtoPrinter;
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

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();

        var main = context.getBean(App.class);
        main.stop();
    }

    @Override
    public synchronized void status(PortalStatusRequest request,
                                    StreamObserver<PortalStatusResponse> response)
    {
        LOG.debug("Request portal slots status: { portalId: {}, request: {} }", portalId,
            ProtoPrinter.safePrinter().printToString(request));
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

        var op = Operation.create(portalId, "Obtain portal slots status: " +
            String.join(", ", request.getSlotNamesList()), null, idempotencyKey, null);
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
        LOG.debug("Open portal slots: { portalId: {}, request: {} }", portalId,
            ProtoPrinter.safePrinter().printToString(request));
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
            try {
                openSlots(request);
            } catch (SnapshotNotFound e) {
                LOG.error("Error while opening portal slot: " + e.getMessage(), e);
                operationService.updateError(op.id(), Status.NOT_FOUND.withDescription(e.getMessage()));
                replyError.accept(e.getMessage(), Status.NOT_FOUND);
                return;
            } catch (SnapshotUniquenessException | NotImplementedException e) {
                LOG.error("Error while opening portal slot: " + e.getMessage(), e);
                operationService.updateError(op.id(), Status.INVALID_ARGUMENT.withDescription(e.getMessage()));
                replyError.accept(e.getMessage(), Status.INVALID_ARGUMENT);
                return;
            } catch (CreateSlotException e) {
                LOG.error("Error while opening portal slot: " + e.getMessage(), e);
                operationService.updateError(op.id(), Status.INTERNAL.withDescription(e.getMessage()));
                replyError.accept(e.getMessage(), Status.INTERNAL);
                return;
            } catch (Exception e) {
                LOG.error("Unexpected error while opening portal slot: " + e.getMessage(), e);
                operationService.updateError(op.id(), Status.INTERNAL.withDescription(e.getMessage()));
                replyError.accept(e.getMessage(), Status.INTERNAL);
                return;
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

    public void openSlots(OpenSlotsRequest request) throws CreateSlotException {
        for (LzyPortal.PortalSlotDesc slotDesc : request.getSlotsList()) {
            LOG.info("Open slot { portalId: {}, slotDesc: {} }", portalId, portalSlotToSafeString(slotDesc));

            final Slot slot = ProtoConverter.fromProto(slotDesc.getSlot());
            if (Slot.STDIN.equals(slot) || Slot.ARGS.equals(slot) ||
                Slot.STDOUT.equals(slot) || Slot.STDERR.equals(slot))
            {
                throw new CreateSlotException("Invalid slot " + slot.name());
            }

            String taskId;
            if (slotDesc.getKindCase().equals(PortalSlotDesc.KindCase.SNAPSHOT)) {
                taskId = portalId;
            } else {
                throw new NotImplementedException(slotDesc.getKindCase().name());
            }

            final SlotInstance slotInstance = new SlotInstance(slot, taskId, slotDesc.getChannelId(),
                slotsService.getSlotsManager().resolveSlotUri(taskId, slot.name()));

            LzySlot newLzySlot;
            if (slotDesc.getKindCase().equals(PortalSlotDesc.KindCase.SNAPSHOT)) {
                newLzySlot = slotsService.getSnapshots().createSlot(slotDesc.getSnapshot(), slotInstance);
            } else {
                throw new NotImplementedException(slotDesc.getKindCase().name());
            }

            try {
                slotsService.getSlotsManager().registerSlot(newLzySlot);
            } catch (RuntimeException e) {
                throw new CreateSlotException("Cannot register slot in slots manager", e);
            }
        }
    }

    @Override
    public void finish(FinishRequest request, StreamObserver<LongRunning.Operation> response) {
        LOG.debug("Finish portal: { portalId: {}, request: {} }", portalId,
            ProtoPrinter.safePrinter().printToString(request));
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
        LocalOperationServiceUtils.awaitOpAndReply(operationService, opId, response, respType, errorMsg, LOG);
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

        if (slotDesc.getKindCase().equals(PortalSlotDesc.KindCase.SNAPSHOT)) {
            sb.append("\"snapshot/uri:").append(slotDesc.getSnapshot().getStorageConfig().getUri())
                .append("\"");
        } else {
            sb.append("\"").append(slotDesc.getKindCase()).append("\"");
        }

        return sb.append("}").toString();
    }
}
