package ai.lzy.servant.portal;

import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.SlotConnectionManager;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class Portal extends LzyFsGrpc.LzyFsImplBase {
    private static final Logger LOG = LogManager.getLogger(Portal.class);

    private final LzyFsServer fs;
    private final String portalTaskId;
    private final Map<String, SnapshotSlot> snapshots = new HashMap<>(); // snapshotId -> in & out slots
    private final Map<String, String> slot2snapshot = new HashMap<>();
    private final AtomicBoolean active = new AtomicBoolean(false);

    public Portal(String servantId, LzyFsServer fs) {
        this.fs = fs;
        this.portalTaskId = "portal:" + UUID.randomUUID() + "@" + servantId;
    }

    public boolean start() {
        if (active.compareAndSet(false, true)) {
            var prev = fs.setSlotApiInterceptor(this);
            assert prev == null;
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

            switch (slotDesc.getStorageCase()) {
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

                default -> throw new NotImplementedException(slotDesc.getStorageCase().name());
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

            final URI remoteSlotUri = URI.create(request.getSlotUri());
            try {
                var bytes = SlotConnectionManager.connectToSlot(remoteSlotUri, 0); // grpc call inside
                ss.getInputSlot().connect(remoteSlotUri, bytes);

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

            return;
        }

        LOG.error("Only snapshot is supported now");
        response.onError(Status.UNIMPLEMENTED.asException());
    }

    @Override
    public synchronized void disconnectSlot(LzyFsApi.DisconnectSlotRequest request,
                                            StreamObserver<LzyFsApi.SlotCommandStatus> response) {
        LOG.info("Disconnect portal slot, taskId: {}, slotName: {}", request.getTaskId(), request.getSlotName());

        assert request.getTaskId().isEmpty() : request.getTaskId();
        var slotName = request.getSlotName();

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

            response.onNext(LzyFsApi.SlotCommandStatus.newBuilder()
                .setRc(LzyFsApi.SlotCommandStatus.RC.newBuilder()
                    .setCode(LzyFsApi.SlotCommandStatus.RC.Code.SUCCESS)
                    .build())
                .build());
            response.onCompleted();
            return;
        }

        LOG.error("Only snapshot is supported now");
        response.onError(Status.UNIMPLEMENTED.asException());
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

            response.onNext(LzyFsApi.SlotCommandStatus.newBuilder()
                .setRc(LzyFsApi.SlotCommandStatus.RC.newBuilder()
                    .setCode(LzyFsApi.SlotCommandStatus.RC.Code.SUCCESS)
                    .build())
                .build());
            response.onCompleted();
            return;
        }

        LOG.error("Only snapshot is supported now");
        response.onError(Status.UNIMPLEMENTED.asException());
    }

    @Override
    public synchronized void openOutputSlot(LzyFsApi.SlotRequest request, StreamObserver<LzyFsApi.Message> response) {
        LOG.info("Open portal output slot, uri: {}, offset: {}", request.getSlotUri(), request.getOffset());
        var slotUri = URI.create(request.getSlotUri());
        var slotName = slotUri.getPath().substring(portalTaskId.length() + 1);

        var snapshotId = slot2snapshot.get(slotName);
        if (snapshotId != null) {
            var ss = snapshots.get(snapshotId);
            if (ss == null) {
                LOG.error("Slot '{}' belongs to snapshot '{}', which is unknown", slotName, snapshotId);
                response.onError(Status.INTERNAL.withDescription("snapshot " + snapshotId + " broken").asException());
                return;
            }

            var outputSlot = ss.getOutputSlot(slotName);
            if (outputSlot == null) {
                LOG.error("Slot '{}' belongs to snapshot '{}', which is unknown", slotName, snapshotId);
                response.onError(Status.NOT_FOUND
                    .withDescription("Slot " + slotName + " is not bound to snapshot " + snapshotId).asException());
                return;
            }

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

            return;
        }

        LOG.error("Only snapshot is supported now");
        response.onError(Status.UNIMPLEMENTED.asException());
    }

    private static String portalSlotToSafeString(PortalSlotDesc slotDesc) {
        var sb = new StringBuilder()
                .append("PortalSlotDesc{")
                .append("\"slot\": ").append(JsonUtils.printSingleLine(slotDesc))
                .append(", \"storage\": ");

        switch (slotDesc.getStorageCase()) {
            case SNAPSHOT ->
                sb.append("\"snapshot/").append(slotDesc.getSnapshot().getId()).append("\"");
            case AMAZONS3 ->
                sb.append("\"amazon-s3\": \"").append(slotDesc.getAmazonS3().getEndpoint()).append("\"");
            default -> sb.append("\"").append(slotDesc.getStorageCase()).append("\"");
        }

        return sb.append("}").toString();
    }
}
