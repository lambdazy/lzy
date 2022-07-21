package ai.lzy.fs;

import ai.lzy.model.*;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.serce.jnrfuse.FuseException;
import ai.lzy.fs.commands.BuiltinCommandHolder;
import ai.lzy.fs.fs.LzyFSManager;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyLinuxFsManagerImpl;
import ai.lzy.fs.fs.LzyMacosFsManagerImpl;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzyScript;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.logs.MetricEvent;
import ai.lzy.model.logs.MetricEventLogger;
import ai.lzy.model.utils.SessionIdInterceptor;
import ai.lzy.v1.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static ai.lzy.model.Constants.LOGS_DIR;
import static ai.lzy.model.UriScheme.*;

public final class LzyFsServer {

    private static final Logger LOG = LogManager.getLogger(LzyFsServer.class);
    public static final AtomicInteger mounted = new AtomicInteger(); //for tests

    public static final String DEFAULT_MOUNT_POINT = "/tmp/lzy";
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8901;

    private final String sessionId;
    private final Path mountPoint;
    private final LzyFSManager fs;
    private final URI selfUri;
    private final URI lzyServerUri;
    private final IAM.Auth auth;
    private final SlotsManager slotsManager;
    private final ManagedChannel lzyServerChannel;
    private final SlotConnectionManager slotConnectionManager;
    private final Server localServer;
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final AtomicReference<LzyFsGrpc.LzyFsImplBase> slotApiInterceptor = new AtomicReference<>(null);

    public LzyFsServer(String sessionId, String mountPoint, URI selfUri, URI lzyServerUri, URI lzyWhiteboardUri,
                       IAM.Auth auth) throws IOException {
        assert LzyFs.scheme().equals(selfUri.getScheme());

        this.sessionId = sessionId;
        this.mountPoint = Path.of(mountPoint);
        fs = startFuse();

        this.selfUri = selfUri;
        this.lzyServerUri = lzyServerUri;
        this.auth = auth;

        LOG.info("Starting LzyFs gRPC server at {}.", selfUri);
        localServer = NettyServerBuilder.forAddress(new InetSocketAddress(selfUri.getHost(), selfUri.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(ServerInterceptors.intercept(new Impl(), new SessionIdInterceptor()))
            .build();
        localServer.start();

        slotsManager = new SlotsManager(sessionId, selfUri);

        // TODO: remove it from LzyFs
        // <<<
        lzyServerChannel = ChannelBuilder
            .forAddress(lzyServerUri.getHost(), lzyServerUri.getPort())
            .usePlaintext()
            .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();
        final LzyServerGrpc.LzyServerBlockingStub lzyServerClient = LzyServerGrpc.newBlockingStub(lzyServerChannel);

        final String bucket = lzyServerClient
            .getBucket(Lzy.GetBucketRequest.newBuilder().setAuth(auth).build())
            .getBucket();
        final Lzy.GetS3CredentialsResponse credentials = lzyServerClient
            .getS3Credentials(Lzy.GetS3CredentialsRequest.newBuilder()
                .setAuth(auth)
                .setBucket(bucket)
                .build());

        slotConnectionManager = new SlotConnectionManager(credentials, auth, lzyWhiteboardUri, bucket, sessionId);
        // >>>

        LOG.info("Registering lzy commands...");
        for (BuiltinCommandHolder command : BuiltinCommandHolder.values()) {
            registerBuiltinCommand(Path.of(command.name()), command.name());
        }

        LOG.info("LzyFs started on {}.", selfUri);
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
                lzyServerChannel.shutdown();
                localServer.shutdown();
            } finally {
                fs.umount();
                mounted.decrementAndGet();
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
        fs.addSlot(slot);
    }

    @Nullable
    public LzyFsGrpc.LzyFsImplBase setSlotApiInterceptor(LzyFsGrpc.LzyFsImplBase interceptor) {
        return slotApiInterceptor.getAndSet(interceptor);
    }

    public LzyFsApi.SlotCommandStatus createSlot(LzyFsApi.CreateSlotRequest request) {
        LOG.info("LzyFsServer::createSlot: taskId={}, slotName={}: {}.",
            request.getTaskId(), request.getSlot().getName(), JsonUtils.printRequest(request));

        var existing = slotsManager.slot(request.getTaskId(), request.getSlot().getName());
        if (existing != null) {
            return onSlotError("Slot `" + request.getSlot().getName() + "` already exists.");
        }

        final Slot slotSpec = GrpcConverter.from(request.getSlot());
        final LzySlot lzySlot = slotsManager.getOrCreateSlot(request.getTaskId(), slotSpec, request.getChannelId());

        // TODO: It will be removed after creating Portal
        final URI channelUri = URI.create(request.getChannelId());
        if (Objects.equals(channelUri.getScheme(), "snapshot") && lzySlot instanceof LzyOutputSlot) {
            if (slotConnectionManager.snapshooter() == null) {
                return onSlotError("Snapshot service was not initialized. Operation is not available.");
            }
            String entryId = request.getChannelId();
            String snapshotId = "snapshot://" + channelUri.getHost();
            slotConnectionManager.snapshooter().registerSlot(lzySlot, snapshotId, entryId);
        }

        if (lzySlot instanceof LzyFileSlot) {
            fs.addSlot((LzyFileSlot) lzySlot);
        }

        return LzyFsApi.SlotCommandStatus.newBuilder().build();
    }

    public LzyFsApi.SlotCommandStatus connectSlot(LzyFsApi.ConnectSlotRequest request) {
        final String slotName = request.getTaskId() + request.getSlotName();
        LOG.info("LzyFsServer::connectSlot `{}`: {}.", slotName, JsonUtils.printRequest(request));

        final LzySlot slot = slotsManager.slot(request.getTaskId(), request.getSlotName());
        if (slot == null) {
            return onSlotError("Slot `" + slotName + "` not found.");
        }

        final URI slotUri = URI.create(request.getSlotUri());
        if (slot instanceof LzyInputSlot) {
            if (SlotS3.match(slotUri) || SlotAzure.match(slotUri)) {
                ((LzyInputSlot) slot).connect(slotUri, slotConnectionManager.connectToS3(slotUri, 0));
            } else {
                ((LzyInputSlot) slot).connect(slotUri, SlotConnectionManager.connectToSlot(slotUri, 0));
            }
            return LzyFsApi.SlotCommandStatus.newBuilder().build();
        }

        return onSlotError("Slot " + request.getSlotName() + " not found in " + request.getTaskId());
    }

    public LzyFsApi.SlotCommandStatus disconnectSlot(LzyFsApi.DisconnectSlotRequest request) {
        final String slotName = request.getTaskId() + request.getSlotName();
        LOG.info("LzyFsServer::disconnectSlot `{}`: {}.", slotName, JsonUtils.printRequest(request));

        final LzySlot slot = slotsManager.slot(request.getTaskId(), request.getSlotName());
        if (slot == null) {
            return onSlotError("Slot `" + slotName + "` not found.");
        }

        slot.suspend();

        return LzyFsApi.SlotCommandStatus.newBuilder().build();
    }

    public LzyFsApi.SlotCommandStatus statusSlot(LzyFsApi.StatusSlotRequest request) {
        final String slotName = request.getTaskId() + request.getSlotName();
        LOG.info("LzyFsServer::statusSlot `{}`: {}.", slotName, JsonUtils.printRequest(request));

        final LzySlot slot = slotsManager.slot(request.getTaskId(), request.getSlotName());
        if (slot == null) {
            return onSlotError("Slot `" + slotName + "` not found.");
        }

        final Operations.SlotStatus.Builder status = Operations.SlotStatus.newBuilder(slot.status());
        status.setTaskId(request.getTaskId());

        return LzyFsApi.SlotCommandStatus.newBuilder()
            .setStatus(status.build())
            .build();
    }

    public LzyFsApi.SlotCommandStatus destroySlot(LzyFsApi.DestroySlotRequest request) {
        final String slotName = request.getTaskId() + request.getSlotName();
        LOG.info("LzyFsServer::destroySlot `{}`: {}.", slotName, JsonUtils.printRequest(request));

        final LzySlot slot = slotsManager.slot(request.getTaskId(), request.getSlotName());
        if (slot == null) {
            return onSlotError("Slot `" + slotName + "` not found.");
        }

        slot.destroy();
        fs.removeSlot(slot.name());
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
        final Path path = Paths.get(URI.create(request.getSlotUri()).getPath());

        if (path.getNameCount() < 2) {
            responseObserver.onError(
                Status.INVALID_ARGUMENT.withDescription("Wrong slot format, must be [task]/[slot]").asException());
            return;
        }

        final String task = path.getName(0).toString();
        // TODO(artolord) Make better slot name resolving
        final String slotName = "/" + path.getName(0).relativize(Path.of("/").relativize(path));
        LOG.debug("task: {}, slot: {}.", task, slotName);

        final LzySlot slot = slotsManager.slot(task, slotName);
        if (!(slot instanceof LzyOutputSlot outputSlot)) {
            LOG.info("Trying to read from input slot " + path);
            responseObserver
                .onError(Status.NOT_FOUND.withDescription("Reading from input slot: " + path).asException());
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
                    "session_id", sessionId,
                    "task_id", task,
                    "metric_type", "system_metric"
                ),
                finish - start
            )
        );
    }

    public boolean registerCommand(Path cmd, String script, @Nullable Operations.Zygote zygote) {
        LOG.debug("Registering command `{}`...", cmd);

        boolean added = fs.addScript(new LzyScriptImpl(cmd, script, zygote), /* isSystem */ zygote == null);

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
        } catch (FuseException e) {
            fs.umount();
            throw e;
        }
        mounted.incrementAndGet();

        return fs;
    }

    private void registerBuiltinCommand(Path cmd, String name, String... args) {
        final List<String> commandParts = new ArrayList<>();
        commandParts.add(System.getProperty("java.home") + "/bin/java");
        commandParts.add("-Xmx1g");
        commandParts.add("-Dcustom.log.file=" + LOGS_DIR + "/" + name + "_$(($RANDOM % 10000))");
        commandParts.add("-Dlog4j.configurationFile=servant_cmd/log4j2.yaml");
        commandParts.add("-classpath");
        commandParts.add('"' + System.getProperty("java.class.path") + '"');
        commandParts.add(BashApi.class.getCanonicalName());
        commandParts.addAll(List.of("--lzy-address", lzyServerUri.getHost() + ":" + lzyServerUri.getPort()));
        //commandParts.addAll(List.of("--lzy-whiteboard", whiteboardAddress.toString()));
        commandParts.addAll(List.of("--lzy-mount", mountPoint.toAbsolutePath().toString()));
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
        if (fs.addScript(new LzyScriptImpl(cmd, script, null), /* isSystem */ true)) {
            LOG.debug("Register command `{}`.", name);
        } else {
            LOG.warn("Command `{}` already exists.", name);
        }
    }


    private record LzyScriptImpl(
        Path location,
        CharSequence scriptText,
        Operations.Zygote zygote
    ) implements LzyScript {

        @Override
        public Zygote operation() {
            return GrpcConverter.from(zygote);
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
            var interceptor = slotApiInterceptor.get();
            if (interceptor == null) {
                slotCall(req, resp, LzyFsServer.this::createSlot);
            } else {
                interceptor.createSlot(req, resp);
            }
        }

        @Override
        public void connectSlot(LzyFsApi.ConnectSlotRequest req, StreamObserver<LzyFsApi.SlotCommandStatus> resp) {
            var interceptor = slotApiInterceptor.get();
            if (interceptor == null) {
                slotCall(req, resp, LzyFsServer.this::connectSlot);
            } else {
                interceptor.connectSlot(req, resp);
            }
        }

        @Override
        public void disconnectSlot(LzyFsApi.DisconnectSlotRequest req, StreamObserver<LzyFsApi.SlotCommandStatus> rsp) {
            var interceptor = slotApiInterceptor.get();
            if (interceptor == null) {
                slotCall(req, rsp, LzyFsServer.this::disconnectSlot);
            } else {
                interceptor.disconnectSlot(req, rsp);
            }
        }

        @Override
        public void statusSlot(LzyFsApi.StatusSlotRequest req, StreamObserver<LzyFsApi.SlotCommandStatus> resp) {
            var interceptor = slotApiInterceptor.get();
            if (interceptor == null) {
                slotCall(req, resp, LzyFsServer.this::statusSlot);
            } else {
                interceptor.statusSlot(req, resp);
            }
        }

        @Override
        public void destroySlot(LzyFsApi.DestroySlotRequest req, StreamObserver<LzyFsApi.SlotCommandStatus> resp) {
            var interceptor = slotApiInterceptor.get();
            if (interceptor == null) {
                slotCall(req, resp, LzyFsServer.this::destroySlot);
            } else {
                interceptor.destroySlot(req, resp);
            }
        }

        @Override
        public void openOutputSlot(LzyFsApi.SlotRequest req, StreamObserver<LzyFsApi.Message> resp) {
            var interceptor = slotApiInterceptor.get();
            if (interceptor == null) {
                LzyFsServer.this.openOutputSlot(req, resp);
            } else {
                interceptor.openOutputSlot(req, resp);
            }
        }
    }
}
