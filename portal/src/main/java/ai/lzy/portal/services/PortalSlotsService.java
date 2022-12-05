package ai.lzy.portal.services;

import ai.lzy.fs.SlotsManager;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.config.PortalConfig;
import ai.lzy.portal.slots.SnapshotProvider;
import ai.lzy.portal.slots.StdoutSlot;
import ai.lzy.util.grpc.ContextAwareTask;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.slots.LSA;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import static ai.lzy.model.UriScheme.LzyFs;
import static ai.lzy.portal.services.PortalService.APP;
import static ai.lzy.portal.services.PortalService.PORTAL_SLOT_PREFIX;
import static ai.lzy.util.grpc.GrpcUtils.*;
import static ai.lzy.v1.channel.LzyChannelManagerGrpc.newBlockingStub;

@Singleton
@Named("PortalSlotsService")
public class PortalSlotsService extends LzySlotsApiGrpc.LzySlotsApiImplBase {
    private static final Logger LOG = LogManager.getLogger(PortalSlotsService.class);

    private final String portalId;
    private final PortalConfig config;

    private StdoutSlot stdoutSlot;
    private StdoutSlot stderrSlot;
    private final SnapshotProvider snapshots;
    private final SlotsManager slotsManager;

    private final LocalOperationService operationService;
    private final ExecutorService workersPool;

    public PortalSlotsService(PortalConfig config, SnapshotProvider snapshots,
                              @Named("PortalTokenSupplier") Supplier<String> tokenFactory,
                              @Named("PortalChannelManagerChannel") ManagedChannel channelsManagerChannel,
                              @Named("PortalOperationsService") LocalOperationService operationService,
                              @Named("PortalServiceExecutor") ExecutorService workersPool)
    {
        this.portalId = config.getPortalId();
        this.config = config;

        this.snapshots = snapshots;
        this.slotsManager = new SlotsManager(
            newBlockingClient(newBlockingStub(channelsManagerChannel), APP, tokenFactory),
            URI.create("%s://%s:%d".formatted(LzyFs.scheme(), config.getHost(), config.getSlotsApiPort())));

        this.operationService = operationService;
        this.workersPool = workersPool;
    }

    public SlotsManager getSlotsManager() {
        return slotsManager;
    }

    public void start() {
        LOG.info("Registering portal stdout/err slots with config: {}", config.toSafeString());

        var stdoutSlotName = PORTAL_SLOT_PREFIX + ":" + Slot.STDOUT_SUFFIX;
        var stderrSlotName = PORTAL_SLOT_PREFIX + ":" + Slot.STDERR_SUFFIX;

        stdoutSlot = new StdoutSlot(stdoutSlotName, portalId, config.getStdoutChannelId(),
            slotsManager.resolveSlotUri(portalId, stdoutSlotName));
        slotsManager.registerSlot(stdoutSlot);

        stderrSlot = new StdoutSlot(stderrSlotName, portalId, config.getStderrChannelId(),
            slotsManager.resolveSlotUri(portalId, stderrSlotName));
        slotsManager.registerSlot(stderrSlot);

        LOG.info("Portal stdout/err slots successfully registered...");
    }

    public void stop() throws InterruptedException {
        this.slotsManager.close();
    }

    public LzyInputSlot findOutSlot(String name) {
        return stdoutSlot.find(name);
    }

    public LzyInputSlot findErrSlot(String name) {
        return stderrSlot.find(name);
    }

    public StdoutSlot getStdoutSlot() {
        return stdoutSlot;
    }

    public StdoutSlot getStderrSlot() {
        return stderrSlot;
    }

    public List<StdoutSlot> getOutErrSlots() {
        if (stdoutSlot != null && stderrSlot != null) {
            return List.of(stdoutSlot, stderrSlot);
        }
        return Collections.emptyList();
    }

    public SnapshotProvider getSnapshots() {
        return snapshots;
    }

    @Override
    public synchronized void connectSlot(LSA.ConnectSlotRequest request,
                                         StreamObserver<LongRunning.Operation> response)
    {
        final SlotInstance from = ProtoConverter.fromProto(request.getFrom());
        final SlotInstance to = ProtoConverter.fromProto(request.getTo());
        LOG.info("Connect portal slot, taskId: {}, slotName: {}, remoteSlotUri: {}",
            from.taskId(), from.name(), to.uri());

        Consumer<LzyInputSlot> startConnect = inputSlot -> {
            try {
                var op = new Operation(
                    portalId,
                    "ConnectSlot: %s -> %s".formatted(from.shortDesc(), to.shortDesc()),
                    Any.pack(LSA.ConnectSlotMetadata.getDefaultInstance())
                );

                operationService.registerOperation(op);

                response.onNext(op.toProto());
                response.onCompleted();

                // TODO: MDC & GrpcConntext
                workersPool.submit(new ContextAwareTask() {
                    @Override
                    protected void execute() {
                        LOG.info("[{}] Trying to connect slots, {} -> {}...",
                            op.id(), from.shortDesc(), to.shortDesc());

                        try {
                            var channel = newGrpcChannel(to.uri().getHost(), to.uri().getPort(),
                                LzySlotsApiGrpc.SERVICE_NAME);
                            var client = newBlockingClient(LzySlotsApiGrpc.newBlockingStub(channel), "PortalSlots",
                                NO_AUTH_TOKEN);

                            var req = LSA.SlotDataRequest.newBuilder()
                                .setSlotInstance(request.getTo())
                                .setOffset(0)
                                .build();

                            var msgIter = client.openOutputSlot(req);

                            var dataProvider = StreamSupport
                                .stream(Spliterators.spliteratorUnknownSize(msgIter, Spliterator.NONNULL), false)
                                .map(msg -> msg.hasChunk() ? msg.getChunk() : ByteString.EMPTY)
                                .onClose(channel::shutdownNow);

                            inputSlot.connect(to.uri(), dataProvider);

                            synchronized (op) {
                                op.setResponse(Any.pack(LSA.ConnectSlotResponse.getDefaultInstance()));
                            }
                            LOG.info("[{}] ... connected", op.id());
                        } catch (Exception e) {
                            LOG.error("[{}] Cannot connect slots, {} -> {}: {}",
                                op.id(), from.shortDesc(), to.shortDesc(), e.getMessage(), e);
                            synchronized (op) {
                                op.setError(Status.INTERNAL.withDescription(e.getMessage()));
                            }
                        }
                    }
                });
            } catch (StatusRuntimeException e) {
                LOG.error("Failed to connect to remote slot: {}", e.getMessage(), e);
                response.onError(Status.ABORTED.withCause(e).asException());
            }
        };

        LzyInputSlot lzyInputSlot = snapshots.getInputSlot(from.name());
        if (lzyInputSlot != null) {
            if (lzyInputSlot.name().equals(from.name())) {
                startConnect.accept(lzyInputSlot);
                return;
            }

            LOG.error("Got connect to unexpected slot '{}', expected input slot '{}'",
                from.name(), lzyInputSlot.name());
            response.onError(Status.INVALID_ARGUMENT.withDescription("Unexpected slot").asException());
            return;
        }

        var stdoutPeerSlot = findOutSlot(from.name());
        if (stdoutPeerSlot != null) {
            startConnect.accept(stdoutPeerSlot);
            return;
        }

        var stderrPeerSlot = findErrSlot(from.name());
        if (stderrPeerSlot != null) {
            startConnect.accept(stderrPeerSlot);
            return;
        }

        LOG.error("Only snapshot is supported now, got connect from `{}` to `{}`", from, to);
        response.onError(Status.INVALID_ARGUMENT.withDescription("Only snapshot is supported now").asException());
    }

    @Override
    public synchronized void disconnectSlot(LSA.DisconnectSlotRequest request,
                                            StreamObserver<LSA.DisconnectSlotResponse> response)
    {
        final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
        LOG.info("Disconnect portal slot, taskId: {}, slotName: {}", slotInstance.taskId(), slotInstance.name());

        var slotName = slotInstance.name();

        boolean done = false;

        LzyInputSlot inputSlot = snapshots.getInputSlot(slotName);
        LzyOutputSlot outputSlot = snapshots.getOutputSlot(slotName);
        if (inputSlot != null) {
            inputSlot.disconnect();
            done = true;
        }
        if (outputSlot != null) {
            outputSlot.suspend();
            done = true;
        }

        if (!done) {
            inputSlot = findOutSlot(slotName);
            if (inputSlot != null) {
                inputSlot.disconnect();
                done = true;
            }
        }

        if (!done) {
            inputSlot = findErrSlot(slotName);
            if (inputSlot != null) {
                inputSlot.disconnect();
                done = true;
            }
        }

        StdoutSlot out = getStdoutSlot();
        if (!done && out.name().equals(slotName)) {
            out.suspend();
            done = true;
        }

        StdoutSlot err = getStderrSlot();
        if (!done && err.name().equals(slotName)) {
            err.suspend();
            done = true;
        }

        if (done) {
            response.onNext(LSA.DisconnectSlotResponse.getDefaultInstance());
            response.onCompleted();
            return;
        }

        LOG.error("Only snapshot or stdout/stderr are supported now, got {}", slotInstance);
        response.onError(Status.NOT_FOUND
            .withDescription("Cannot find slot " + slotName).asException());
    }

    @Override
    public synchronized void statusSlot(LSA.StatusSlotRequest request,
                                        StreamObserver<LSA.StatusSlotResponse> response)
    {
        final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
        LOG.info("Status portal slot, taskId: {}, slotName: {}", slotInstance.taskId(), slotInstance.name());

        if (!portalId.equals(slotInstance.taskId())) {
            LOG.error("Unknown task " + slotInstance.taskId());
            response.onError(Status.INVALID_ARGUMENT
                .withDescription("Unknown task " + slotInstance.taskId()).asException());
            return;
        }

        Consumer<LzySlot> reply = slot -> {
            response.onNext(
                LSA.StatusSlotResponse.newBuilder()
                    .setStatus(slot.status())
                    .build());
            response.onCompleted();
        };

        for (var slot : snapshots.getInputSlots()) {
            if (slot.name().equals(slotInstance.name())) {
                reply.accept(slot);
                return;
            }
        }

        for (var slot : snapshots.getOutputSlots()) {
            reply.accept(slot);
            return;
        }

        for (var stdSlot : getOutErrSlots()) {
            if (stdSlot.name().equals(slotInstance.name())) {
                reply.accept(stdSlot);
                return;
            }

            var slot = stdSlot.find(slotInstance.name());
            if (slot != null) {
                reply.accept(slot);
                return;
            }
        }

        LOG.error("Slot '" + slotInstance.name() + "' not found");
        response.onError(Status.NOT_FOUND
            .withDescription("Slot '" + slotInstance.name() + "' not found").asException());
    }

    @Override
    public synchronized void destroySlot(LSA.DestroySlotRequest request,
                                         StreamObserver<LSA.DestroySlotResponse> response)
    {
        final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
        LOG.info("Destroy portal slot, taskId: {}, slotName: {}", slotInstance.taskId(), slotInstance.name());
        var slotName = slotInstance.name();

        boolean done = false;

        LzyInputSlot inputSlot = snapshots.getInputSlot(slotName);
        LzyOutputSlot outputSlot = snapshots.getOutputSlot(slotName);
        if (inputSlot != null) {
            inputSlot.destroy();
            snapshots.removeInputSlot(slotName);
            done = true;
        }
        if (outputSlot != null) {
            outputSlot.destroy();
            snapshots.removeOutputSlot(slotName);
            done = true;
        }

        if (!done) {
            inputSlot = findOutSlot(slotName);
            if (inputSlot != null) {
                inputSlot.destroy();
                done = true;
            }
        }

        if (!done) {
            inputSlot = findErrSlot(slotName);
            if (inputSlot != null) {
                inputSlot.destroy();
                done = true;
            }
        }

        StdoutSlot out = getStdoutSlot();
        if (!done && out.name().equals(slotName)) {
            out.destroy();
            done = true;
        }

        StdoutSlot err = getStderrSlot();
        if (!done && err.name().equals(slotName)) {
            err.destroy();
            done = true;
        }

        if (done) {
            response.onNext(LSA.DestroySlotResponse.getDefaultInstance());
            response.onCompleted();
            return;
        }

        LOG.error("Only snapshot or stdout/stderr are supported now, got {}", slotInstance);
        response.onError(Status.NOT_FOUND
            .withDescription("Cannot find slot " + slotName).asException());
    }

    @Override
    public void openOutputSlot(LSA.SlotDataRequest request, StreamObserver<LSA.SlotDataChunk> response) {
        final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
        LOG.info("Open portal output slot, uri: {}, offset: {}", slotInstance.uri(), request.getOffset());
        final var slotName = slotInstance.name();

        Consumer<LzyOutputSlot> reader = outputSlot -> {
            try {
                outputSlot
                    .readFromPosition(request.getOffset())
                    .forEach(chunk -> response.onNext(LSA.SlotDataChunk.newBuilder().setChunk(chunk).build()));

                response.onNext(LSA.SlotDataChunk.newBuilder().setControl(LSA.SlotDataChunk.Control.EOS).build());
                response.onCompleted();
            } catch (Exception e) {
                LOG.error("Error while uploading data: {}", e.getMessage(), e);
                response.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
            }
        };

        LzyOutputSlot outputSlot;

        synchronized (this) {
            outputSlot = snapshots.getOutputSlot(slotName);
            if (outputSlot == null) {
                StdoutSlot out = getStdoutSlot();
                StdoutSlot err = getStderrSlot();
                if (out.name().equals(slotName)) {
                    outputSlot = out;
                } else if (err.name().equals(slotName)) {
                    outputSlot = err;
                }
            }
        }

        if (outputSlot != null) {
            reader.accept(outputSlot);
            return;
        }

        LOG.error("Only snapshot or stdout/stderr are supported now, got {}", slotInstance);
        response.onError(Status.INVALID_ARGUMENT
            .withDescription("Only snapshot or stdout/stderr are supported now").asException());
    }
}
