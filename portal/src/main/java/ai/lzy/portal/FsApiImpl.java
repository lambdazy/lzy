package ai.lzy.portal;

import ai.lzy.fs.SlotConnectionManager;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.deprecated.GrpcConverter;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.slots.SnapshotSlotsProvider;
import ai.lzy.portal.slots.StdoutSlot;
import ai.lzy.v1.fs.LzyFsApi;
import ai.lzy.v1.fs.LzyFsApi.ConnectSlotRequest;
import ai.lzy.v1.fs.LzyFsApi.SlotCommandStatus;
import ai.lzy.v1.fs.LzyFsGrpc.LzyFsImplBase;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.function.Consumer;

class FsApiImpl extends LzyFsImplBase {
    private static final Logger LOG = LogManager.getLogger(FsApiImpl.class);

    private final Portal portal;

    FsApiImpl(Portal portal) {
        this.portal = portal;
    }

    @Override
    public void createSlot(LzyFsApi.CreateSlotRequest request, StreamObserver<SlotCommandStatus> response) {
        response.onError(Status.UNIMPLEMENTED.withDescription("Not supported in portal").asException());
    }

    @Override
    public synchronized void connectSlot(ConnectSlotRequest request, StreamObserver<SlotCommandStatus> response) {
        final SlotInstance from = ProtoConverter.fromProto(request.getFrom());
        final SlotInstance to = ProtoConverter.fromProto(request.getTo());
        LOG.info("Connect portal slot, taskId: {}, slotName: {}, remoteSlotUri: {}",
            from.taskId(), from.name(), to.uri());

        var slotName = from.name();
        var remoteSlotUri = to.uri();

        Consumer<LzyInputSlot> doConnect = inputSlot -> {
            try {
                var bytes = SlotConnectionManager.connectToSlot(to, 0); // grpc call inside
                inputSlot.connect(remoteSlotUri, bytes);

                response.onNext(SlotCommandStatus.newBuilder()
                    .setRc(SlotCommandStatus.RC.newBuilder()
                        .setCode(SlotCommandStatus.RC.Code.SUCCESS)
                        .build())
                    .build());
                response.onCompleted();
            } catch (StatusRuntimeException e) {
                LOG.error("Failed to connect to remote slot: {}", e.getMessage(), e);
                response.onError(Status.ABORTED.withCause(e).asException());
            }
        };

        LzyInputSlot lzyInputSlot = portal.getSnapshots().getInputSlot(slotName);
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

        var stdoutPeerSlot = portal.findOutSlot(slotName);
        if (stdoutPeerSlot != null) {
            doConnect.accept(stdoutPeerSlot);
            return;
        }

        var stderrPeerSlot = portal.findErrSlot(slotName);
        if (stderrPeerSlot != null) {
            doConnect.accept(stderrPeerSlot);
            return;
        }

        LOG.error("Only snapshot is supported now");
        response.onError(Status.UNIMPLEMENTED.asException());
    }

    @Override
    public synchronized void disconnectSlot(LzyFsApi.DisconnectSlotRequest request,
                                            StreamObserver<SlotCommandStatus> response) {
        final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
        LOG.info("Disconnect portal slot, taskId: {}, slotName: {}", slotInstance.taskId(), slotInstance.name());

        assert slotInstance.taskId().isEmpty() : slotInstance.taskId();
        var slotName = slotInstance.name();

        boolean done = false;

        SnapshotSlotsProvider snapshots = portal.getSnapshots();
        LzyInputSlot inputSlot = snapshots.getInputSlot(slotName);
        LzyOutputSlot outputSlot = snapshots.getOutputSlot(slotName);
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
            inputSlot = portal.findOutSlot(slotName);
            if (inputSlot != null) {
                inputSlot.disconnect();
                done = true;
            }
        }

        if (!done) {
            inputSlot = portal.findErrSlot(slotName);
            if (inputSlot != null) {
                inputSlot.disconnect();
                done = true;
            }
        }

        StdoutSlot out = portal.getStdoutSlot();
        if (!done && out.name().equals(slotName)) {
            out.suspend();
            done = true;
        }

        StdoutSlot err = portal.getStderrSlot();
        if (!done && err.name().equals(slotName)) {
            err.suspend();
            done = true;
        }

        if (done) {
            response.onNext(SlotCommandStatus.newBuilder()
                .setRc(SlotCommandStatus.RC.newBuilder()
                    .setCode(SlotCommandStatus.RC.Code.SUCCESS)
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
                                        StreamObserver<SlotCommandStatus> response) {
        final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
        LOG.info("Status portal slot, taskId: {}, slotName: {}", slotInstance.taskId(), slotInstance.name());

        if (!portal.getPortalTaskId().equals(slotInstance.taskId())) {
            response.onError(Status.INVALID_ARGUMENT
                .withDescription("Unknown task " + slotInstance.taskId()).asException());
            return;
        }

        Consumer<LzySlot> reply = slot -> {
            response.onNext(SlotCommandStatus.newBuilder()
                .setStatus(slot.status())
                .build());
            response.onCompleted();
        };

        SnapshotSlotsProvider snapshots = portal.getSnapshots();
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

        for (var stdSlot : portal.getOutErrSlots()) {
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
                                         StreamObserver<SlotCommandStatus> response) {
        final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
        LOG.info("Destroy portal slot, taskId: {}, slotName: {}", slotInstance.taskId(), slotInstance.name());
        var slotName = slotInstance.name();

        boolean done = false;

        SnapshotSlotsProvider snapshots = portal.getSnapshots();
        LzyInputSlot inputSlot = snapshots.getInputSlot(slotName);
        LzyOutputSlot outputSlot = snapshots.getOutputSlot(slotName);
        if (inputSlot != null) {
            inputSlot.destroy();
            snapshots.removeInputSlot(slotName);
        }
        if (outputSlot != null) {
            outputSlot.destroy();
            snapshots.removeOutputSlot(slotName);
        }
        if (inputSlot != null || outputSlot != null) {
            done = true;
        }

        if (!done) {
            inputSlot = portal.findOutSlot(slotName);
            if (inputSlot != null) {
                inputSlot.destroy();
                done = true;
            }
        }

        if (!done) {
            inputSlot = portal.findErrSlot(slotName);
            if (inputSlot != null) {
                inputSlot.destroy();
                done = true;
            }
        }

        StdoutSlot out = portal.getStdoutSlot();
        if (!done && out.name().equals(slotName)) {
            out.destroy();
            done = true;
        }

        StdoutSlot err = portal.getStderrSlot();
        if (!done && err.name().equals(slotName)) {
            err.destroy();
            done = true;
        }

        if (done) {
            response.onNext(SlotCommandStatus.newBuilder()
                .setRc(SlotCommandStatus.RC.newBuilder()
                    .setCode(SlotCommandStatus.RC.Code.SUCCESS)
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
        final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
        LOG.info("Open portal output slot, uri: {}, offset: {}", slotInstance.uri(), request.getOffset());
        final var slotUri = slotInstance.uri();
        final var slotName = slotUri.getPath().substring(portal.getPortalTaskId().length() + 1);

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
            } catch (Exception e) {
                LOG.error("Error while uploading data: {}", e.getMessage(), e);
                response.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
            }
        };

        LzyOutputSlot outputSlot;

        synchronized (this) {
            outputSlot = portal.getSnapshots().getOutputSlot(slotName);
            if (outputSlot == null) {
                StdoutSlot out = portal.getStdoutSlot();
                StdoutSlot err = portal.getStderrSlot();
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

        LOG.error("Only snapshot or stdout/stderr are supported now");
        response.onError(Status.UNIMPLEMENTED.asException());
    }
}
