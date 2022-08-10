package ai.lzy.servant.portal;

import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.SlotConnectionManager;
import ai.lzy.fs.SlotsManager;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.slots.LzySlotBase;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.Slot;
import ai.lzy.model.SlotInstance;
import ai.lzy.servant.portal.ExternalStorage.AmazonS3Key;
import ai.lzy.servant.portal.ExternalStorage.AzureS3Key;
import ai.lzy.servant.portal.ExternalStorage.S3RepositoryProvider;
import ai.lzy.servant.portal.s3.S3StorageOutputSlot;
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
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
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
    // local snapshots
    private final Map<String, SnapshotSlot> snapshots = new HashMap<>(); // snapshotId -> in & out slots
    private final Map<String, String> slot2snapshot = new HashMap<>(); // slotName -> snapshotId
    // external storage
    private final ExternalStorage externalStorage = new ExternalStorage();
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

            final Slot slot = GrpcConverter.from(slotDesc.getSlot());
            final String taskId = switch (slotDesc.getKindCase()) {
                case STDERR -> slotDesc.getStderr().getTaskId();
                case STDOUT -> slotDesc.getStdout().getTaskId();
                case SNAPSHOT, STOREDONS3 -> portalTaskId;
                default -> throw new RuntimeException("unknown slot kind");
            };
            final SlotInstance slotInstance = new SlotInstance(
                slot,
                taskId,
                slotDesc.getChannelId(),
                fs.getSlotsManager().resolveSlotUri(taskId, slot.name())
            );

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

                                lzySlot = ss.setInputSlot(slotInstance);
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

                            lzySlot = ss.addOutputSlot(slotInstance);
                        }

                        default -> {
                            return replyError.apply("Unknown slot direction " + slot.direction());
                        }
                    }

                    fs.getSlotsManager().registerSlot(lzySlot);
                }

                case STDOUT, STDERR -> {
                    final boolean stdout = slotDesc.getKindCase() == PortalSlotDesc.KindCase.STDOUT;
                    var lzySlot = (stdout ? stdoutSlot : stderrSlot).attach(slotInstance);
                    if (lzySlot == null) {
                        return replyError.apply("Slot " + slot.name() + " from task " + taskId + " already exists");
                    }
                    fs.getSlotsManager().registerSlot(lzySlot);
                }

                case STOREDONS3 -> {
                    var s3SnapshotData = slotDesc.getStoredOnS3();
                    String key = s3SnapshotData.getS3Key();
                    String bucket = s3SnapshotData.getS3Bucket();

                    S3RepositoryProvider clientProvider = switch (s3SnapshotData.getS3EndpointCase()) {
                        case AMAZONSTYLE -> AmazonS3Key.of(s3SnapshotData.getAmazonStyle().getEndpoint(),
                                s3SnapshotData.getAmazonStyle().getAccessToken(),
                                s3SnapshotData.getAmazonStyle().getSecretToken());
                        case AZURESTYLE -> AzureS3Key.of(s3SnapshotData.getAzureStyle().getConnectionString());
                        default -> null;
                    };
                    if (clientProvider == null) {
                        return replyError.apply("Unknown s3 endpoint type " + s3SnapshotData.getS3EndpointCase());
                    }

                    LzySlot lzySlot = switch (slot.direction()) {
                        case INPUT -> externalStorage.createSlotSnapshot(slotInstance, key, bucket, clientProvider);
                        case OUTPUT -> {
                            S3StorageOutputSlot slot1 = externalStorage.readSlotSnapshot(slotInstance, key, bucket, clientProvider);
                            slot1.open();
                            yield slot1;
                        }
                    };
                    if (lzySlot != null) {
                        fs.getSlotsManager().registerSlot(lzySlot);
                    } else {
                        return replyError.apply("Unknown slot direction " + slot.direction());
                    }
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

        for (var input : externalStorage.getInputSlots()) {
            response.addSlots(
                    LzyPortalApi.PortalSlotStatus.newBuilder()
                            .setSlot(GrpcConverter.to(input.definition()))
                            .setConnectedTo(Optional.ofNullable(input.connectedTo()).map(URI::toString).orElse(""))
                            .setState(input.state())
                            .build());
        }

        for (var output : externalStorage.getOutputSlots()) {
            response.addSlots(
                    LzyPortalApi.PortalSlotStatus.newBuilder()
                            .setSlot(GrpcConverter.to(output.definition()))
                            .setConnectedTo("")
                            .setState(output.state())
                            .build());
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

        var s3SnapshotInputSlot = externalStorage.getInputSlot(slotName);
        if (s3SnapshotInputSlot != null) {
            doConnect.accept(s3SnapshotInputSlot);
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

        if (!done) {
            LzyInputSlot inputSlot = externalStorage.getInputSlot(slotName);
            if (inputSlot != null) {
                inputSlot.disconnect();
            }
            LzyOutputSlot outputSlot = externalStorage.getOutputSlot(slotName);
            if (outputSlot != null) {
                outputSlot.suspend();
            }

            if (inputSlot == null && outputSlot == null) {
                LOG.error("Got disconnect from unexpected slot '{}'", slotName);
                response.onError(Status.INVALID_ARGUMENT.withDescription("Unexpected slot").asException());
            } else {
                done = true;
            }
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

        for (var ss : snapshots.values()) {
            var inputSlot = ss.getInputSlot();
            if (inputSlot != null && inputSlot.name().equals(slotInstance.name())) {
                reply.accept(inputSlot);
                return;
            }

            for (var outputSlot : ss.getOutputSlots()) {
                if (outputSlot.name().equals(slotInstance.name())) {
                    reply.accept(outputSlot);
                    return;
                }
            }
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

        LzySlot slot = externalStorage.getInputSlot(slotInstance.name());
        if (slot == null) {
            slot = externalStorage.getOutputSlot(slotInstance.name());
        }
        if (slot != null) {
            reply.accept(slot);
            return;
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

        if (!done) {
            LzyInputSlot inputSlot = externalStorage.getInputSlot(slotName);
            if (inputSlot != null) {
                inputSlot.destroy();
                externalStorage.removeInputSlot(inputSlot.name());
            }
            LzyOutputSlot outputSlot = externalStorage.getOutputSlot(slotName);
            if (outputSlot != null) {
                outputSlot.destroy();
                externalStorage.removeOutputSlot(outputSlot.name());
            }

            if (inputSlot == null && outputSlot == null) {
                LOG.error("Got destroy from unexpected slot '{}'", slotName);
                response.onError(Status.INVALID_ARGUMENT.withDescription("Unexpected slot").asException());
            } else {
                done = true;
            }
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

        LzyOutputSlot outputSlot = null;

        synchronized (this) {
            var snapshotId = slot2snapshot.get(slotName);
            if (snapshotId != null) {
                var ss = snapshots.get(snapshotId);
                if (ss == null) {
                    LOG.error("Slot '{}' belongs to snapshot '{}', which is unknown", slotName, snapshotId);
                    response.onError(Status.INTERNAL
                        .withDescription("snapshot " + snapshotId + " broken").asException());
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
            } else {
                outputSlot = externalStorage.getOutputSlot(slotName);
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
            case SNAPSHOT -> sb.append("\"snapshot/").append(slotDesc.getSnapshot().getId()).append("\"");
            case STDOUT -> sb.append("\"stdout\"");
            case STDERR -> sb.append("\"stderr\"");
            default -> sb.append("\"").append(slotDesc.getKindCase()).append("\"");
        }

        return sb.append("}").toString();
    }
}
