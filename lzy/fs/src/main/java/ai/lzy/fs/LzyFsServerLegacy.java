package ai.lzy.fs;

import ai.lzy.fs.commands.BuiltinCommandHolder;
import ai.lzy.fs.fs.*;
import ai.lzy.logs.MetricEvent;
import ai.lzy.logs.MetricEventLogger;
import ai.lzy.model.deprecated.Zygote;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.deprecated.Lzy;
import ai.lzy.v1.deprecated.LzyAuth;
import ai.lzy.v1.deprecated.LzyFsApi;
import ai.lzy.v1.deprecated.LzyFsGrpc;
import ai.lzy.v1.deprecated.LzyKharonGrpc;
import ai.lzy.v1.deprecated.LzyServerGrpc;
import ai.lzy.v1.deprecated.LzyZygote;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.jsonwebtoken.Jwts;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.serce.jnrfuse.FuseException;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

import static ai.lzy.model.Constants.LOGS_DIR;
import static ai.lzy.model.UriScheme.LzyFs;
import static ai.lzy.model.UriScheme.SlotAzure;
import static ai.lzy.model.UriScheme.SlotS3;
import static ai.lzy.model.deprecated.GrpcConverter.from;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;
import static ai.lzy.v1.channel.LzyChannelManagerGrpc.newBlockingStub;

public final class LzyFsServerLegacy {

    private static final Logger LOG = LogManager.getLogger(LzyFsServerLegacy.class);
    public static final AtomicInteger mounted = new AtomicInteger(); //for tests

    private final String agentId;
    private final Path mountPoint;
    private final URI selfUri;
    private final URI lzyServerUri;
    private final URI lzyWhiteboardUri;
    private final URI channelManagerUri;

    private final LzyAuth.Auth auth;

    private final Server localServer;

    private SlotsManager slotsManager;
    private ManagedChannel channelManagerChannel;
    private SlotConnectionManager slotConnectionManager;

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private LzyFSManager fsManager;

    @Deprecated
    public LzyFsServerLegacy(String agentId, String mountPoint, URI selfUri, @Nullable URI lzyServerUri,
                             @Nullable URI lzyWhiteboardUri, URI channelManagerUri, LzyAuth.Auth auth)
    {
        this.agentId = agentId;
        this.channelManagerUri = channelManagerUri;
        assert LzyFs.scheme().equals(selfUri.getScheme());
        this.mountPoint = Path.of(mountPoint);
        this.selfUri = selfUri;
        this.lzyServerUri = lzyServerUri;
        this.auth = auth;
        this.lzyWhiteboardUri = lzyWhiteboardUri;

        localServer = newGrpcServer(selfUri.getHost(), selfUri.getPort(), GrpcUtils.NO_AUTH)
            .addService(new Impl())
            .build();
    }

    public LzyFsServerLegacy(String agentId, String mountPoint, URI selfUri, URI channelManagerUri, String token)
        throws IOException
    {
        this(agentId, mountPoint, selfUri, null, null, channelManagerUri, LzyAuth.Auth.newBuilder()
            .setUser(LzyAuth.UserCredentials.newBuilder()
                .setToken(token)
                .build())
            .build());
    }

    public void start() throws IOException {
        fsManager = startFuse();

        LOG.info("Starting LzyFs gRPC server at {}.", selfUri);
        localServer.start();

        channelManagerChannel = newGrpcChannel(channelManagerUri.getHost(), channelManagerUri.getPort(),
            LzyChannelManagerGrpc.SERVICE_NAME);
        slotsManager = new SlotsManager(
            newBlockingClient(
                newBlockingStub(channelManagerChannel),
                "LzyFs",
                () -> auth.hasUser()
                    ? auth.getUser().getToken()
                    : generateJwtServantToken(auth.getTask().getServantId())
            ),
            selfUri
        );

        // TODO: remove it from LzyFs
        // <<<

        if (lzyServerUri != null) {
            final var lzyServerChannel = newGrpcChannel(lzyServerUri.getHost(), lzyServerUri.getPort(),
                LzyKharonGrpc.SERVICE_NAME);
            final var lzyServerClient = newBlockingClient(LzyServerGrpc.newBlockingStub(lzyServerChannel), "Fs", null);

            final String bucket = lzyServerClient
                .getBucket(Lzy.GetBucketRequest.newBuilder().setAuth(auth).build())
                .getBucket();
            final Lzy.GetS3CredentialsResponse credentials = lzyServerClient
                .getS3Credentials(Lzy.GetS3CredentialsRequest.newBuilder()
                    .setAuth(auth)
                    .setBucket(bucket)
                    .build());

            lzyServerChannel.shutdown();

            slotConnectionManager = new SlotConnectionManager(credentials, auth, lzyWhiteboardUri, bucket);
        } else {
            slotConnectionManager = new SlotConnectionManager();
        }
        // >>>

        LOG.info("Registering lzy commands...");
        for (BuiltinCommandHolder command : BuiltinCommandHolder.values()) {
            registerBuiltinCommand(Path.of(command.name()), command.name());
        }

        LOG.info("LzyFs started on {}.", selfUri);
    }

    private String generateJwtServantToken(String workerId) {
        final Instant now = Instant.now();
        return Jwts.builder()
            .setIssuedAt(Date.from(now))
            .setNotBefore(Date.from(now))
            .setExpiration(Date.from(now.plusSeconds(Duration.ofDays(7).toSeconds())))
            .setIssuer(workerId)
            .compact();
    }

    public Path getMountPoint() {
        return mountPoint;
    }

    public URI getUri() {
        return selfUri;
    }

    public SlotConnectionManager getSlotConnectionManager() {
        return slotConnectionManager;
    }

    public SlotsManager getSlotsManager() {
        return slotsManager;
    }

    public void stop() {
        LOG.info("LzyFs shutdown request at {}, path {}", selfUri, mountPoint);
        if (stopped.compareAndSet(false, true)) {
            try {
                channelManagerChannel.shutdown();
                localServer.shutdown();
            } finally {
                try {
                    fsManager.umount();
                } finally {
                    mounted.decrementAndGet();
                }
            }
        }
    }

    public void awaitTermination() throws InterruptedException, IOException {
        LOG.info("LzyFs awaiting termination at {}.", selfUri);
        try {
            if (slotConnectionManager.snapshooter() != null) {
                slotConnectionManager.snapshooter().close();
            }
            slotsManager.close();
        } finally {
            stop();
        }
        LOG.info("LzyFs at {} terminated.", selfUri);
    }

    public void addSlot(LzyFileSlot slot) {
        LOG.info("Explicitly add slot: {}", slot.name());
        fsManager.addSlot(slot);
    }

    public LzyFsApi.SlotCommandStatus createSlot(LzyFsApi.CreateSlotRequest request) {
        LOG.info("LzyFsServer::createSlot: taskId={}, slotName={}: {}.",
            request.getTaskId(), request.getSlot().getName(), JsonUtils.printRequest(request));

        var existing = slotsManager.slot(request.getTaskId(), request.getSlot().getName());
        if (existing != null) {
            return onSlotError("Slot `" + request.getSlot().getName() + "` already exists.");
        }

        final Slot slotSpec = ProtoConverter.fromProto(request.getSlot());
        final LzySlot lzySlot = slotsManager.getOrCreateSlot(request.getTaskId(), slotSpec, request.getChannelId());

        // TODO: It will be removed after creating Portal
        final String channelName = request.getChannelId().split("!")[1];
        final URI channelUri = URI.create(channelName);
        if (Objects.equals(channelUri.getScheme(), "snapshot") && lzySlot instanceof LzyOutputSlot) {
            if (slotConnectionManager.snapshooter() == null) {
                return onSlotError("Snapshot service was not initialized. Operation is not available.");
            }
            String entryId = channelName;
            String snapshotId = "snapshot://" + channelUri.getHost();
            slotConnectionManager.snapshooter().registerSlot(lzySlot, snapshotId, entryId);
        }

        if (lzySlot instanceof LzyFileSlot) {
            fsManager.addSlot((LzyFileSlot) lzySlot);
        }

        return LzyFsApi.SlotCommandStatus.getDefaultInstance();
    }

    public LzyFsApi.SlotCommandStatus connectSlot(LzyFsApi.ConnectSlotRequest request) {
        final SlotInstance fromSlot = ProtoConverter.fromProto(request.getFrom());
        final String slotName = fromSlot.taskId() + fromSlot.name();
        LOG.info("LzyFsServer::connectSlot `{}`: {}.", slotName, JsonUtils.printRequest(request));

        final LzySlot slot = slotsManager.slot(fromSlot.taskId(), fromSlot.name());
        if (slot == null) {
            return onSlotError("Slot `" + slotName + "` not found.");
        }

        final LMS.SlotInstance to = request.getTo();
        final URI slotUri = URI.create(to.getSlotUri());
        if (slot instanceof LzyInputSlot inputSlot) {
            if (SlotS3.match(slotUri) || SlotAzure.match(slotUri)) {
                inputSlot.connect(slotUri, slotConnectionManager.connectToS3(slotUri, 0));
            } else {
                inputSlot.connect(slotUri, SlotConnectionManager.connectToSlot(ProtoConverter.fromProto(to), 0));
            }
            return LzyFsApi.SlotCommandStatus.getDefaultInstance();
        }

        return onSlotError("Slot " + fromSlot.spec().name() + " not found in " + fromSlot.taskId());
    }

    public LzyFsApi.SlotCommandStatus disconnectSlot(LzyFsApi.DisconnectSlotRequest request) {
        final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
        final String slotName = slotInstance.taskId() + slotInstance.name();
        LOG.info("LzyFsServer::disconnectSlot `{}`: {}.", slotName, JsonUtils.printRequest(request));

        final LzySlot slot = slotsManager.slot(slotInstance.taskId(), slotInstance.name());
        if (slot == null) {
            return onSlotError("Slot `" + slotName + "` not found.");
        }

        slot.suspend();

        return LzyFsApi.SlotCommandStatus.getDefaultInstance();
    }

    public LzyFsApi.SlotCommandStatus statusSlot(LzyFsApi.StatusSlotRequest request) {
        final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
        final String slotName = slotInstance.taskId() + slotInstance.name();
        LOG.info("LzyFsServer::statusSlot `{}`: {}.", slotName, JsonUtils.printRequest(request));

        final LzySlot slot = slotsManager.slot(slotInstance.taskId(), slotInstance.name());
        if (slot == null) {
            return onSlotError("Slot `" + slotName + "` not found.");
        }

        final LMS.SlotStatus.Builder status = LMS.SlotStatus.newBuilder(slot.status());
        status.setTaskId(slotInstance.taskId());

        return LzyFsApi.SlotCommandStatus.newBuilder()
            .setStatus(status.build())
            .build();
    }

    public LzyFsApi.SlotCommandStatus destroySlot(LzyFsApi.DestroySlotRequest request) {
        final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
        final String slotName = slotInstance.taskId() + slotInstance.name();
        LOG.info("LzyFsServer::destroySlot `{}`: {}.", slotName, JsonUtils.printRequest(request));

        final LzySlot slot = slotsManager.slot(slotInstance.taskId(), slotInstance.name());
        if (slot == null) {
            return onSlotError("Slot `" + slotName + "` not found.");
        }

        slot.destroy();
        fsManager.removeSlot(slot.name());
        LOG.info("Explicitly closing slot tid: {}", slotName);

        return LzyFsApi.SlotCommandStatus.newBuilder().build();
    }

    private static LzyFsApi.SlotCommandStatus onSlotError(String message) {
        return LzyFsApi.SlotCommandStatus.newBuilder()
            .setRc(LzyFsApi.SlotCommandStatus.RC.newBuilder()
                .setCode(LzyFsApi.SlotCommandStatus.RC.Code.ERROR)
                .setDescription(message)
                .build())
            .build();
    }

    public void openOutputSlot(LzyFsApi.SlotRequest request, StreamObserver<LzyFsApi.Message> responseObserver) {
        LOG.info("LzyFsServer::openOutputSlot {}.", JsonUtils.printRequest(request));

        final long start = System.currentTimeMillis();
        final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
        final String taskId = slotInstance.taskId();
        LOG.debug("taskId: {}, slot: {}.", taskId, slotInstance.name());

        final LzySlot slot = slotsManager.slot(taskId, slotInstance.name());
        if (!(slot instanceof LzyOutputSlot outputSlot)) {
            LOG.info("Trying to read from input slot " + slotInstance.uri());
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("Reading from input slot: " + slotInstance.uri())
                .asException());
            return;
        }

        try {
            outputSlot.readFromPosition(request.getOffset())
                .forEach(chunk -> responseObserver.onNext(LzyFsApi.Message.newBuilder().setChunk(chunk).build()));
            responseObserver.onNext(LzyFsApi.Message.newBuilder().setControl(LzyFsApi.Message.Controls.EOS).build());
            responseObserver.onCompleted();
        } catch (IOException iae) {
            responseObserver.onError(iae);
        }

        final long finish = System.currentTimeMillis();
        MetricEventLogger.log(
            new MetricEvent(
                "LzyServant openOutputSlot time",
                Map.of(
                    "task_id", taskId,
                    "metric_type", "system_metric"
                ),
                finish - start
            )
        );
    }

    public boolean registerCommand(Path cmd, String script, @Nullable LzyZygote.Zygote zygote) {
        LOG.debug("Registering command `{}`...", cmd);

        boolean added = fsManager.addScript(new LzyScriptImpl(cmd, script, zygote), /* isSystem */ zygote == null);

        if (added) {
            LOG.debug("Command `{}` registered.", cmd);
        } else {
            LOG.warn("Command `{}` already exists.", cmd);
        }

        return added;
    }

    private LzyFSManager startFuse() {
        LzyFSManager fs;
        if (SystemUtils.IS_OS_MAC) {
            fs = new LzyMacosFsManagerImpl();
        } else if (SystemUtils.IS_OS_LINUX) {
            fs = new LzyLinuxFsManagerImpl();
        } else {
            throw new RuntimeException(SystemUtils.OS_NAME + " is not supported");
        }

        LOG.info("Mounting LzyFs at {}.", mountPoint.toAbsolutePath().toString());
        try {
            fs.mount(mountPoint);
            mounted.incrementAndGet();
        } catch (FuseException e) {
            fs.umount();
            throw e;
        }

        return fs;
    }

    private void registerBuiltinCommand(Path cmd, String name, String... args) {
        final List<String> commandParts = new ArrayList<>();
        commandParts.add(System.getProperty("java.home") + "/bin/java");
        commandParts.add("-Xmx1g");
        commandParts.add("-Dcustom.log.file=" + LOGS_DIR + "/" + name + "_$(($RANDOM % 10000))");
        commandParts.add("-Dlog4j.configurationFile=worker_cmd/log4j2.yaml");
        commandParts.add("-classpath");
        commandParts.add('"' + System.getProperty("java.class.path") + '"');
        commandParts.add(BashApi.class.getCanonicalName());
        if (lzyServerUri != null) {
            commandParts.addAll(List.of("--lzy-address", lzyServerUri.toString()));
        }
        commandParts.addAll(List.of("--channel-manager",
            channelManagerUri.getHost() + ":" + channelManagerUri.getPort()));
        //commandParts.addAll(List.of("--lzy-whiteboard", whiteboardAddress.toString()));
        commandParts.addAll(List.of("--lzy-mount", mountPoint.toAbsolutePath().toString()));
        commandParts.addAll(List.of("--agent-id", agentId));
        commandParts.addAll(List.of("--lzy-fs-port", Integer.toString(selfUri.getPort())));
        // TODO: move it to Env
        commandParts.addAll(List.of(
            "--auth",
            new String(Base64.getEncoder().encode(auth.toByteString().toByteArray()))
        ));
        commandParts.add(name);
        commandParts.addAll(Arrays.asList(args));
        commandParts.add("$@");

        final String script = String.join(" ", commandParts) + "\n";
        if (fsManager.addScript(new LzyScriptImpl(cmd, script, null), /* isSystem */ true)) {
            LOG.debug("Register command `{}`.", name);
        } else {
            LOG.warn("Command `{}` already exists.", name);
        }
    }

    private record LzyScriptImpl(
        Path location,
        CharSequence scriptText,
        LzyZygote.Zygote zygote
    ) implements LzyScript {

        @Override
        public Zygote operation() {
            return from(zygote);
        }
    }

    private final class Impl extends LzyFsGrpc.LzyFsImplBase {

        private interface SlotFn<R> {

            LzyFsApi.SlotCommandStatus call(R req);
        }

        private <R> void slotCall(R req, StreamObserver<LzyFsApi.SlotCommandStatus> resp, SlotFn<R> fn) {
            try {
                var status = fn.call(req);
                resp.onNext(status);
                resp.onCompleted();
            } catch (Exception e) {
                resp.onError(e);
            }
        }

        @Override
        public void createSlot(LzyFsApi.CreateSlotRequest req, StreamObserver<LzyFsApi.SlotCommandStatus> resp) {
            slotCall(req, resp, LzyFsServerLegacy.this::createSlot);
        }

        @Override
        public void connectSlot(LzyFsApi.ConnectSlotRequest req, StreamObserver<LzyFsApi.SlotCommandStatus> resp) {
            slotCall(req, resp, LzyFsServerLegacy.this::connectSlot);
        }

        @Override
        public void disconnectSlot(LzyFsApi.DisconnectSlotRequest req, StreamObserver<LzyFsApi.SlotCommandStatus> rsp) {
            slotCall(req, rsp, LzyFsServerLegacy.this::disconnectSlot);
        }

        @Override
        public void statusSlot(LzyFsApi.StatusSlotRequest req, StreamObserver<LzyFsApi.SlotCommandStatus> resp) {
            slotCall(req, resp, LzyFsServerLegacy.this::statusSlot);
        }

        @Override
        public void destroySlot(LzyFsApi.DestroySlotRequest req, StreamObserver<LzyFsApi.SlotCommandStatus> resp) {
            slotCall(req, resp, LzyFsServerLegacy.this::destroySlot);
        }

        @Override
        public void openOutputSlot(LzyFsApi.SlotRequest req, StreamObserver<LzyFsApi.Message> resp) {
            LzyFsServerLegacy.this.openOutputSlot(req, resp);
        }
    }
}
