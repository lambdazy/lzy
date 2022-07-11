package ai.lzy.servant.portal;

import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.SlotConnectionManager;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.slots.LzySlotBase;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.Slot;
import ai.lzy.priv.v2.LzyFsApi;
import ai.lzy.priv.v2.LzyFsGrpc;
import ai.lzy.priv.v2.LzyPortalApi;
import ai.lzy.priv.v2.LzyPortalApi.OpenSlotsRequest;
import ai.lzy.priv.v2.LzyPortalApi.OpenSlotsResponse;
import ai.lzy.priv.v2.LzyPortalApi.PortalSlotDesc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.*;
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
    // snapshots
    private final Map<String, SnapshotSlot> snapshots = new HashMap<>(); // snapshotId -> in & out slots
    private final Map<String, String> slot2snapshot = new HashMap<>();
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

            stdoutSlot = new StdoutSlot("stdout");
            fs.getSlotsManager().registerSlot(portalTaskId, stdoutSlot, request.getStdoutChannelId());

            stderrSlot = new StdoutSlot("stderr");
            fs.getSlotsManager().registerSlot(portalTaskId, stderrSlot, request.getStderrChannelId());

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

        var response = OpenSlotsResponse.newBuilder()
                .setSuccess(true);

        final Function<String, OpenSlotsResponse> replyError = message -> {
            LOG.error(message);
            response.setSuccess(false);
            response.setDescription(message);
            return response.build();
        };

        for (PortalSlotDesc slotDesc : request.getSlotsList()) {
            LOG.info("Open slot {}", portalSlotToSafeString(slotDesc));

            var slot = GrpcConverter.from(slotDesc.getSlot());

            if (Slot.STDIN.equals(slot) || Slot.ARGS.equals(slot)
                || Slot.STDOUT.equals(slot) || Slot.STDERR.equals(slot)) {
                return replyError.apply("Invalid slot " + slot);
            }

            switch (slotDesc.getKindCase()) {
                case SNAPSHOT -> {
                    var snapshotId = slotDesc.getSnapshot().getId();

                    var prevSnapshotId = slot2snapshot.putIfAbsent(slotDesc.getSlot().getName(), snapshotId);
                    if (prevSnapshotId != null) {
                        return replyError.apply("Slot '" + slotDesc.getSlot().getName() + "' already associated with "
                            + "snapshot '" + prevSnapshotId + "'");
                    }

                    LzySlot lzySlot;

                    switch (slot.direction()) {
                        case INPUT -> {
                            try {
                                var ss = new SnapshotSlot(snapshotId);
                                var prev = snapshots.put(snapshotId, ss);
                                if (prev != null) {
                                    return replyError.apply("Snapshot '" + snapshotId + "' already exists.");
                                }

                                lzySlot = ss.setInputSlot(portalTaskId, slot);
                            } catch (IOException e) {
                                return replyError.apply("Error file configuring snapshot storage: " + e.getMessage());
                            }
                        }

                        case OUTPUT -> {
                            var ss = snapshots.get(snapshotId);
                            if (ss == null) {
                                return replyError.apply("Attempt to open output snapshot " + snapshotId
                                    + " slot, while input is not set yet");
                            }
                            if (ss.getOutputSlot(slot.name()) != null) {
                                return replyError.apply("Attempt to open output snapshot " + snapshotId
                                    + " slot, while input is not set yet");
                            }

                            lzySlot = ss.addOutputSlot(portalTaskId, slot);
                        }

                        default -> {
                            return replyError.apply("Unknown slot direction " + slot.direction());
                        }
                    }

                    fs.getSlotsManager().registerSlot(portalTaskId, lzySlot, slotDesc.getChannelId());
                }

                case STDOUT, STDERR -> {
                    final boolean stdout = slotDesc.getKindCase() == PortalSlotDesc.KindCase.STDOUT;
                    var taskId = stdout ? slotDesc.getStdout().getTaskId() : slotDesc.getStderr().getTaskId();
                    var lzySlot = (stdout ? stdoutSlot : stderrSlot).attach(taskId, slot);
                    if (lzySlot == null) {
                        return replyError.apply("Slot " + slot.name() + " from task " + taskId + " already exists");
                    }
                    fs.getSlotsManager().registerSlot(portalTaskId, lzySlot, slotDesc.getChannelId());
                }

                default -> throw new NotImplementedException(slotDesc.getKindCase().name());
            }
        }

        return response.build();
    }

    public synchronized LzyPortalApi.PortalStatus status() {
        var response = LzyPortalApi.PortalStatus.newBuilder();

        snapshots.values().forEach(
            ss -> {
                var inputSlot = ss.getInputSlot();
                if (inputSlot != null) {
                    response.addSlots(
                        LzyPortalApi.PortalSlotStatus.newBuilder()
                            .setSlot(GrpcConverter.to(inputSlot.definition()))
                            .setConnectedTo(Optional.ofNullable(inputSlot.connectedTo()).map(URI::toString).orElse(""))
                            .setState(inputSlot.state())
                            .build());
                }

                for (var outputSlot : ss.getOutputSlots()) {
                    response.addSlots(
                        LzyPortalApi.PortalSlotStatus.newBuilder()
                            .setSlot(GrpcConverter.to(outputSlot.definition()))
                            .setConnectedTo("")
                            .setState(outputSlot.state())
                            .build());
                }
            });

        for (var stdSlot : new StdoutSlot[]{stdoutSlot, stderrSlot}) {
            response.addSlots(
                LzyPortalApi.PortalSlotStatus.newBuilder()
                    .setSlot(GrpcConverter.to(stdSlot.definition()))
                    .setConnectedTo("")
                    .setState(stdSlot.state())
                    .build());

            stdSlot.forEachSlot(slot ->
                response.addSlots(
                    LzyPortalApi.PortalSlotStatus.newBuilder()
                        .setSlot(GrpcConverter.to(slot.definition()))
                        .setConnectedTo(Optional.ofNullable(slot.connectedTo()).map(URI::toString).orElse(""))
                        .setState(slot.state())
                        .build()));
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
        LOG.info("Connect portal slot, taskId: {}, slotName: {}, remoteSlotUri: {}",
                request.getTaskId(), request.getSlotName(), request.getSlotUri());

        assert request.getTaskId().isEmpty() : request.getTaskId();
        var slotName = request.getSlotName();
        var remoteSlotUri = URI.create(request.getSlotUri());

        Consumer<LzyInputSlot> doConnect = inputSlot -> {
            try {
                var bytes = SlotConnectionManager.connectToSlot(remoteSlotUri, 0); // grpc call inside
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

        var snapshotId = slot2snapshot.get(slotName);
        if (snapshotId != null) {
            var ss = snapshots.get(snapshotId);
            if (ss == null) {
                LOG.error("Slot '{}' belongs to snapshot '{}', which is unknown", slotName, snapshotId);
                response.onError(Status.INTERNAL
                        .withDescription("snapshot " + snapshotId + " is broken").asException());
                return;
            }

            var inputSlot = ss.getInputSlot();
            if (inputSlot == null || !inputSlot.name().equals(slotName)) {
                LOG.error("Got connect to unexpected slot '{}', snapshotId '{}', expected input slot '{}'",
                    slotName, snapshotId, Optional.ofNullable(inputSlot).map(LzySlotBase::name).orElse("<none>"));
                response.onError(Status.INVALID_ARGUMENT.withDescription("Unexpected slot").asException());
                return;
            }

            doConnect.accept(inputSlot);
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
        assert false;
    }

    @Override
    public synchronized void disconnectSlot(LzyFsApi.DisconnectSlotRequest request,
                                            StreamObserver<LzyFsApi.SlotCommandStatus> response) {
        LOG.info("Disconnect portal slot, taskId: {}, slotName: {}", request.getTaskId(), request.getSlotName());

        assert request.getTaskId().isEmpty() : request.getTaskId();
        var slotName = request.getSlotName();

        boolean done = false;

        var snapshotId = slot2snapshot.get(slotName);
        if (snapshotId != null) {
            var ss = snapshots.get(snapshotId);
            if (ss == null) {
                LOG.error("Slot '{}' belongs to snapshot '{}', which is unknown", slotName, snapshotId);
                response.onError(Status.INTERNAL.withDescription("snapshot " + snapshotId + " broken").asException());
                return;
            }

            var inputSlot = ss.getInputSlot();
            if (inputSlot != null && inputSlot.name().equals(slotName)) {
                inputSlot.disconnect();
            }

            var outputSlot = ss.getOutputSlot(slotName);
            if (outputSlot != null) {
                outputSlot.suspend();
            }

            if (inputSlot == null && outputSlot == null) {
                LOG.error("Got disconnect from unexpected slot '{}', snapshotId '{}'", slotName, snapshotId);
                response.onError(Status.INVALID_ARGUMENT.withDescription("Unexpected slot").asException());
                return;
            }

            done = true;
        }

        if (!done) {
            var inputSlot = stdoutSlot.find(slotName);
            if (inputSlot != null) {
                inputSlot.disconnect();
                done = true;
            }
        }

        if (!done) {
            var inputSlot = stderrSlot.find(slotName);
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
        assert false;
    }

    @Override
    public synchronized void statusSlot(LzyFsApi.StatusSlotRequest request,
                                        StreamObserver<LzyFsApi.SlotCommandStatus> response) {
        // TODO: implement me plz
        super.statusSlot(request, response);
    }

    @Override
    public synchronized void destroySlot(LzyFsApi.DestroySlotRequest request,
                                         StreamObserver<LzyFsApi.SlotCommandStatus> response) {
        LOG.info("Destroy portal slot, taskId: {}, slotName: {}", request.getTaskId(), request.getSlotName());

        assert request.getTaskId().isEmpty() : request.getTaskId();
        var slotName = request.getSlotName();

        boolean done = false;

        var snapshotId = slot2snapshot.get(slotName);
        if (snapshotId != null) {
            var ss = snapshots.get(snapshotId);
            if (ss == null) {
                LOG.error("Slot '{}' belongs to snapshot '{}', which is unknown", slotName, snapshotId);
                response.onError(Status.INTERNAL.withDescription("snapshot " + snapshotId + " broken").asException());
                return;
            }

            var inputSlot = ss.getInputSlot();
            if (inputSlot != null && inputSlot.name().equals(slotName)) {
                inputSlot.destroy();
                ss.removeInputSlot(slotName);
            }

            var outputSlot = ss.getOutputSlot(slotName);
            if (outputSlot != null) {
                outputSlot.destroy();
                ss.removeOutputSlot(slotName);
            }

            if (inputSlot == null && outputSlot == null) {
                LOG.error("Got disconnect from unexpected slot '{}', snapshotId '{}'", slotName, snapshotId);
                response.onError(Status.INVALID_ARGUMENT.withDescription("Unexpected slot").asException());
                return;
            }

            done = true;
        }

        if (!done) {
            var inputSlot = stdoutSlot.find(slotName);
            if (inputSlot != null) {
                inputSlot.destroy();
                done = true;
            }
        }

        if (!done) {
            var inputSlot = stderrSlot.find(slotName);
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
        assert false;
    }

    @Override
    public void openOutputSlot(LzyFsApi.SlotRequest request, StreamObserver<LzyFsApi.Message> response) {
        LOG.info("Open portal output slot, uri: {}, offset: {}", request.getSlotUri(), request.getOffset());
        var slotUri = URI.create(request.getSlotUri());
        var slotName = slotUri.getPath().substring(portalTaskId.length() + 1);

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

        LzyOutputSlot outputSlot = null;

        synchronized (this) {
            var snapshotId = slot2snapshot.get(slotName);
            if (snapshotId != null) {
                var ss = snapshots.get(snapshotId);
                if (ss == null) {
                    LOG.error("Slot '{}' belongs to snapshot '{}', which is unknown", slotName, snapshotId);
                    response.onError(Status.INTERNAL.withDescription("snapshot " + snapshotId + " broken").asException());
                    return;
                }

                outputSlot = ss.getOutputSlot(slotName);
                if (outputSlot == null) {
                    LOG.error("Slot '{}' belongs to snapshot '{}', which is unknown", slotName, snapshotId);
                    response.onError(Status.NOT_FOUND
                        .withDescription("Slot " + slotName + " is not bound to snapshot " + snapshotId).asException());
                    return;
                }
            } else if (stdoutSlot.name().equals(slotName)) {
                outputSlot = stdoutSlot;
            } else if (stderrSlot.name().equals(slotName)) {
                outputSlot = stderrSlot;
            }
        }

        if (outputSlot != null) {
            reader.accept(outputSlot);
            return;
        }

        LOG.error("Only snapshot or stdout/stderr are supported now");
        response.onError(Status.UNIMPLEMENTED.asException());
        assert false;
    }

    private static String portalSlotToSafeString(PortalSlotDesc slotDesc) {
        var sb = new StringBuilder()
                .append("PortalSlotDesc{")
                .append("\"slot\": ").append(JsonUtils.printSingleLine(slotDesc))
                .append(", \"storage\": ");

        switch (slotDesc.getKindCase()) {
            case SNAPSHOT -> sb.append("\"snapshot/").append(slotDesc.getSnapshot().getId()).append("\"");
            case STDOUT -> sb.append("\"stdout\"");
            case STDERR -> sb.append("\"stderr\"");
            //case AMAZONS3 ->
            //    sb.append("\"amazon-s3\": \"").append(slotDesc.getAmazonS3().getEndpoint()).append("\"");
            default -> sb.append("\"").append(slotDesc.getKindCase()).append("\"");
        }

        return sb.append("}").toString();
    }
}
