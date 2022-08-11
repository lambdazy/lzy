package ai.lzy.servant.portal;

import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.SlotConnectionManager;
import ai.lzy.fs.SlotsManager;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.Slot;
import ai.lzy.model.SlotInstance;
import ai.lzy.servant.portal.slots.StdoutSlot;
import ai.lzy.servant.portal.utils.PortalUtils;
import ai.lzy.v1.LzyFsApi;
import ai.lzy.v1.LzyFsGrpc;
import ai.lzy.v1.LzyPortalApi;
import ai.lzy.v1.LzyPortalApi.OpenSlotsRequest;
import ai.lzy.v1.LzyPortalApi.OpenSlotsResponse;
import ai.lzy.v1.LzyPortalApi.PortalSlotDesc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

public class Portal extends LzyFsGrpc.LzyFsImplBase {
    private static final Logger LOG = LogManager.getLogger(Portal.class);

    private final LzyFsServer fs;
    private final String portalTaskId;
    // stdout/stderr (guarded by this)
    private StdoutSlot stdoutSlot = null;
    private StdoutSlot stderrSlot = null;
    private final SnapshotLzySlotsProvider snapshots = new SnapshotLzySlotsProvider();
    // common
    private final AtomicBoolean active = new AtomicBoolean(false);

    public Portal(String servantId, LzyFsServer fs) {
        this.fs = fs;
        this.portalTaskId = "portal:" + UUID.randomUUID() + "@" + servantId;
    }

    public synchronized boolean start(LzyPortalApi.StartPortalRequest request) {
        if (active.compareAndSet(false, true)) {
            var prev = fs.setSlotApiInterceptor(this);
            assert prev == null;

            final String stdoutSlotName = "/portal:stdout";
            final String stderrSlotName = "/portal:stderr";
            final SlotsManager slotsManager = fs.getSlotsManager();
            stdoutSlot = new StdoutSlot(
                stdoutSlotName,
                portalTaskId,
                request.getStdoutChannelId(),
                slotsManager.resolveSlotUri(portalTaskId, stdoutSlotName)
            );
            slotsManager.registerSlot(stdoutSlot);

            stderrSlot = new StdoutSlot(
                stderrSlotName,
                portalTaskId, request.getStderrChannelId(),
                slotsManager.resolveSlotUri(portalTaskId, stderrSlotName)
            );
            slotsManager.registerSlot(stderrSlot);

            return true;
        }
        return false;
    }

    public boolean stop() {
        return active.compareAndSet(true, false);
    }

    public boolean isActive() {
        return active.get();
    }

    public synchronized OpenSlotsResponse openSlots(OpenSlotsRequest request) {
        LOG.info("Configure portal slots request.");

        if (!active.get()) {
            throw new IllegalStateException("Portal is not active.");
        }

        var response = OpenSlotsResponse.newBuilder().setSuccess(true);

        final Function<String, OpenSlotsResponse> replyError = message -> {
            LOG.error(message);
            response.setSuccess(false);
            response.setDescription(message);
            return response.build();
        };

        for (PortalSlotDesc slotDesc : request.getSlotsList()) {
            LOG.info("Open slot {}", portalSlotToSafeString(slotDesc));

            final Slot slot = GrpcConverter.from(slotDesc.getSlot());
            if (Slot.STDIN.equals(slot) || Slot.ARGS.equals(slot)
                || Slot.STDOUT.equals(slot) || Slot.STDERR.equals(slot)) {
                return replyError.apply("Invalid slot " + slot);
            }

            final String taskId = switch (slotDesc.getKindCase()) {
                case ORDINARY -> portalTaskId;
                case STDERR -> slotDesc.getStderr().getTaskId();
                case STDOUT -> slotDesc.getStdout().getTaskId();
                default -> throw new NotImplementedException(slotDesc.getKindCase().name());
            };
            final SlotInstance slotInstance = new SlotInstance(slot, taskId, slotDesc.getChannelId(),
                fs.getSlotsManager().resolveSlotUri(taskId, slot.name()));

            try {
                LzySlot newLzySlot = switch (slotDesc.getKindCase()) {
                    case ORDINARY -> snapshots.createLzySlot(slotDesc.getOrdinary(), slotInstance);
                    case STDOUT -> stdoutSlot.attach(slotInstance);
                    case STDERR -> stderrSlot.attach(slotInstance);
                    default -> throw new NotImplementedException(slotDesc.getKindCase().name());
                };
                fs.getSlotsManager().registerSlot(newLzySlot);
            } catch (CreatingLzySlotException e) {
                replyError.apply(e.getMessage());
            }
        }

        return response.build();
    }

    public synchronized LzyPortalApi.PortalStatus status() {
        var response = LzyPortalApi.PortalStatus.newBuilder();

        snapshots.lzyInputSlots().forEach(slot -> response.addSlots(PortalUtils.buildInputSlotStatus(slot)));
        snapshots.lzyOutputSlots().forEach(slot -> response.addSlots(PortalUtils.buildOutputSlotStatus(slot)));

        for (var stdSlot : new StdoutSlot[]{stdoutSlot, stderrSlot}) {
            response.addSlots(PortalUtils.buildOutputSlotStatus(stdSlot));
            stdSlot.forEachSlot(slot -> response.addSlots(PortalUtils.buildInputSlotStatus(slot)));
        }

        return response.build();
    }

    @Override
    public void createSlot(LzyFsApi.CreateSlotRequest request, StreamObserver<LzyFsApi.SlotCommandStatus> response) {
        response.onError(Status.UNIMPLEMENTED.withDescription("Not supported in portal").asException());
    }

    @Override
    public synchronized void connectSlot(LzyFsApi.ConnectSlotRequest request,
                                         StreamObserver<LzyFsApi.SlotCommandStatus> response) {
        final SlotInstance from = GrpcConverter.from(request.getFrom());
        final SlotInstance to = GrpcConverter.from(request.getTo());
        LOG.info("Connect portal slot, taskId: {}, slotName: {}, remoteSlotUri: {}",
            from.taskId(), from.name(), to.uri());

        var slotName = from.name();
        var remoteSlotUri = to.uri();

        Consumer<LzyInputSlot> doConnect = inputSlot -> {
            try {
                var bytes = SlotConnectionManager.connectToSlot(to, 0); // grpc call inside
                inputSlot.connect(remoteSlotUri, bytes);

                response.onNext(LzyFsApi.SlotCommandStatus.newBuilder()
                    .setRc(LzyFsApi.SlotCommandStatus.RC.newBuilder()
                        .setCode(LzyFsApi.SlotCommandStatus.RC.Code.SUCCESS)
                        .build())
                    .build());
                response.onCompleted();
            } catch (StatusRuntimeException e) {
                LOG.error("Failed to connect to remote slot: {}", e.getMessage(), e);
                response.onError(Status.ABORTED.withCause(e).asException());
            }
        };

        LzyInputSlot lzyInputSlot = snapshots.lzyInputSlot(slotName);
        if (lzyInputSlot != null) {
            if (!lzyInputSlot.name().equals(slotName)) {
                LOG.error("Got connect to unexpected slot '{}', expected input slot '{}'",
                    slotName, Optional.of(lzyInputSlot).map(LzySlot::name).orElse("<none>"));
                response.onError(Status.INVALID_ARGUMENT.withDescription("Unexpected slot").asException());
            } else {
                doConnect.accept(lzyInputSlot);
            }
            return;
        }

        var stdoutPeerSlot = stdoutSlot.find(slotName);
        if (stdoutPeerSlot != null) {
            doConnect.accept(stdoutPeerSlot);
            return;
        }

        var stderrPeerSlot = stderrSlot.find(slotName);
        if (stderrPeerSlot != null) {
            doConnect.accept(stderrPeerSlot);
            return;
        }

        LOG.error("Only snapshot is supported now");
        response.onError(Status.UNIMPLEMENTED.asException());
    }

    @Override
    public synchronized void disconnectSlot(LzyFsApi.DisconnectSlotRequest request,
                                            StreamObserver<LzyFsApi.SlotCommandStatus> response) {
        final SlotInstance slotInstance = GrpcConverter.from(request.getSlotInstance());
        LOG.info("Disconnect portal slot, taskId: {}, slotName: {}", slotInstance.taskId(), slotInstance.name());

        assert slotInstance.taskId().isEmpty() : slotInstance.taskId();
        var slotName = slotInstance.name();

        boolean done = false;

        LzyInputSlot inputSlot = snapshots.lzyInputSlot(slotName);
        LzyOutputSlot outputSlot = snapshots.lzyOutputSlot(slotName);
        if (inputSlot != null) {
            inputSlot.disconnect();
        }
        if (outputSlot != null) {
            outputSlot.suspend();
        }
        if (inputSlot != null || outputSlot != null) {
            done = true;
        }

        if (!done) {
            inputSlot = stdoutSlot.find(slotName);
            if (inputSlot != null) {
                inputSlot.disconnect();
                done = true;
            }
        }

        if (!done) {
            inputSlot = stderrSlot.find(slotName);
            if (inputSlot != null) {
                inputSlot.disconnect();
                done = true;
            }
        }

        if (!done && stdoutSlot.name().equals(slotName)) {
            stdoutSlot.suspend();
            done = true;
        }

        if (!done && stderrSlot.name().equals(slotName)) {
            stderrSlot.suspend();
            done = true;
        }

        if (done) {
            response.onNext(LzyFsApi.SlotCommandStatus.newBuilder()
                .setRc(LzyFsApi.SlotCommandStatus.RC.newBuilder()
                    .setCode(LzyFsApi.SlotCommandStatus.RC.Code.SUCCESS)
                    .build())
                .build());
            response.onCompleted();
            return;
        }

        LOG.error("Only snapshot or stdout/stderr are supported now");
        response.onError(Status.UNIMPLEMENTED.asException());
    }

    @Override
    public synchronized void statusSlot(LzyFsApi.StatusSlotRequest request,
                                        StreamObserver<LzyFsApi.SlotCommandStatus> response) {
        final SlotInstance slotInstance = GrpcConverter.from(request.getSlotInstance());
        LOG.info("Status portal slot, taskId: {}, slotName: {}", slotInstance.taskId(), slotInstance.name());

        if (!portalTaskId.equals(slotInstance.taskId())) {
            response.onError(Status.INVALID_ARGUMENT
                .withDescription("Unknown task " + slotInstance.taskId()).asException());
            return;
        }

        Consumer<LzySlot> reply = slot -> {
            response.onNext(LzyFsApi.SlotCommandStatus.newBuilder()
                .setStatus(slot.status())
                .build());
            response.onCompleted();
        };

        for (var slot : snapshots.lzyInputSlots()) {
            if (slot.name().equals(slotInstance.name())) {
                reply.accept(slot);
                return;
            }
        }

        for (var slot : snapshots.lzyOutputSlots()) {
            reply.accept(slot);
            return;
        }

        for (var stdSlot : new StdoutSlot[]{stdoutSlot, stderrSlot}) {
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

        response.onError(Status.NOT_FOUND
            .withDescription("Slot '" + slotInstance.name() + "' not found").asException());
    }

    @Override
    public synchronized void destroySlot(LzyFsApi.DestroySlotRequest request,
                                         StreamObserver<LzyFsApi.SlotCommandStatus> response) {
        final SlotInstance slotInstance = GrpcConverter.from(request.getSlotInstance());
        LOG.info("Destroy portal slot, taskId: {}, slotName: {}", slotInstance.taskId(), slotInstance.name());
        var slotName = slotInstance.name();

        boolean done = false;

        LzyInputSlot inputSlot = snapshots.lzyInputSlot(slotName);
        LzyOutputSlot outputSlot = snapshots.lzyOutputSlot(slotName);
        if (inputSlot != null) {
            inputSlot.destroy();
            snapshots.removeLzyInputSlot(slotName);
        }
        if (outputSlot != null) {
            outputSlot.destroy();
            snapshots.removeLzyOutputSlot(slotName);
        }
        if (inputSlot != null || outputSlot != null) {
            done = true;
        }

        if (!done) {
            inputSlot = stdoutSlot.find(slotName);
            if (inputSlot != null) {
                inputSlot.destroy();
                done = true;
            }
        }

        if (!done) {
            inputSlot = stderrSlot.find(slotName);
            if (inputSlot != null) {
                inputSlot.destroy();
                done = true;
            }
        }

        if (!done && stdoutSlot.name().equals(slotName)) {
            stdoutSlot.destroy();
            done = true;
        }

        if (!done && stderrSlot.name().equals(slotName)) {
            stderrSlot.destroy();
            done = true;
        }

        if (done) {
            response.onNext(LzyFsApi.SlotCommandStatus.newBuilder()
                .setRc(LzyFsApi.SlotCommandStatus.RC.newBuilder()
                    .setCode(LzyFsApi.SlotCommandStatus.RC.Code.SUCCESS)
                    .build())
                .build());
            response.onCompleted();
            return;
        }

        LOG.error("Only snapshot or stdout/stderr are supported now");
        response.onError(Status.UNIMPLEMENTED.asException());
    }

    @Override
    public void openOutputSlot(LzyFsApi.SlotRequest request, StreamObserver<LzyFsApi.Message> response) {
        final SlotInstance slotInstance = GrpcConverter.from(request.getSlotInstance());
        LOG.info("Open portal output slot, uri: {}, offset: {}", slotInstance.uri(), request.getOffset());
        final var slotUri = slotInstance.uri();
        final var slotName = slotUri.getPath().substring(portalTaskId.length() + 1);

        Consumer<LzyOutputSlot> reader = outputSlot -> {
            try {
                outputSlot.readFromPosition(request.getOffset())
                    .forEach(chunk ->
                        response.onNext(LzyFsApi.Message.newBuilder()
                            .setChunk(chunk)
                            .build()));

                response.onNext(LzyFsApi.Message.newBuilder()
                    .setControl(LzyFsApi.Message.Controls.EOS)
                    .build());
                response.onCompleted();
            } catch (IOException e) {
                LOG.error("Error while uploading data: {}", e.getMessage(), e);
                response.onError(e);
            }
        };

        LzyOutputSlot outputSlot;

        synchronized (this) {
            outputSlot = snapshots.lzyOutputSlot(slotName);
            if (outputSlot == null) {
                if (stdoutSlot.name().equals(slotName)) {
                    outputSlot = stdoutSlot;
                } else if (stderrSlot.name().equals(slotName)) {
                    outputSlot = stderrSlot;
                }
            }
        }

        if (outputSlot != null) {
            reader.accept(outputSlot);
            return;
        }

        LOG.error("Only snapshot or stdout/stderr are supported now");
        response.onError(Status.UNIMPLEMENTED.asException());
    }

    private static String portalSlotToSafeString(PortalSlotDesc slotDesc) {
        var sb = new StringBuilder()
                .append("PortalSlotDesc{")
                .append("\"slot\": ").append(JsonUtils.printSingleLine(slotDesc))
                .append(", \"storage\": ");

        switch (slotDesc.getKindCase()) {
            case ORDINARY -> sb.append("\"snapshot/").append(slotDesc.getOrdinary().getLocalId()).append("\"");
            case STDOUT -> sb.append("\"stdout\"");
            case STDERR -> sb.append("\"stderr\"");
            default -> sb.append("\"").append(slotDesc.getKindCase()).append("\"");
        }

        return sb.append("}").toString();
    }

    public static class CreatingLzySlotException extends Exception {
        public CreatingLzySlotException(String message) {
            super(message);
        }
    }
}
