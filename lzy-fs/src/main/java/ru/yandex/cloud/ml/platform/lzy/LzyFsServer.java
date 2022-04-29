package ru.yandex.cloud.ml.platform.lzy;

import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseException;
import ru.yandex.cloud.ml.platform.lzy.commands.BuiltinLzyCommands;
import ru.yandex.cloud.ml.platform.lzy.fs.*;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
import ru.yandex.cloud.ml.platform.lzy.model.utils.SessionIdInterceptor;
import ru.yandex.cloud.ml.platform.lzy.slots.SlotConnectionManager;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static ru.yandex.cloud.ml.platform.lzy.model.Constants.LOGS_DIR;
import static ru.yandex.cloud.ml.platform.lzy.model.UriScheme.*;

public final class LzyFsServer {
    private static final Logger LOG = LogManager.getLogger(LzyFsServer.class);

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
        for (BuiltinLzyCommands command : BuiltinLzyCommands.values()) {
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
        LOG.info("LzyFs shutdown request at {}.", selfUri);
        lzyServerChannel.shutdown();
        localServer.shutdown();
    }

    public void awaitTermination() throws InterruptedException, IOException {
        LOG.info("LzyFs awaiting termination at {}.", selfUri);
        lzyServerChannel.awaitTermination(30, TimeUnit.SECONDS);
        localServer.awaitTermination();
        slotsManager.close();
        if (slotConnectionManager.snapshooter() != null) {
            slotConnectionManager.snapshooter().close();
        }
        fs.umount();
        LOG.info("LzyFs at {} terminated.", selfUri);
    }

    public void addSlot(LzyFileSlot slot) {
        LOG.info("Explicitly add slot: {}", slot.name());
        fs.addSlot(slot);
    }

    public Servant.SlotCommandStatus configureSlot(Servant.SlotCommand request) throws StatusException {
        final String slotName = request.getTid() + request.getSlot();
        LOG.info("Configure slot `{}`: {}.", slotName, JsonUtils.printRequest(request));

        final LzySlot slot = slotsManager.slot(request.getTid(), request.getSlot());

        if (slot == null && request.getCommandCase() != Servant.SlotCommand.CommandCase.CREATE) {
            return Servant.SlotCommandStatus.newBuilder()
                .setRc(
                    Servant.SlotCommandStatus.RC.newBuilder()
                        .setCodeValue(1)
                        .setDescription("Slot `" + slotName + "` not found.")
                        .build()
                ).build();
        }

        switch (request.getCommandCase()) {
            case CREATE: {
                final Servant.CreateSlotCommand create = request.getCreate();
                final Slot slotSpec = GrpcConverter.from(create.getSlot());
                final LzySlot lzySlot = slotsManager.configureSlot(request.getTid(), slotSpec, create.getChannelId());
                if (lzySlot instanceof LzyFileSlot) {
                    fs.addSlot((LzyFileSlot) lzySlot);
                }
                break;
            }

            case SNAPSHOT: {
                if (slotConnectionManager.snapshooter() == null) {
                    return Servant.SlotCommandStatus.newBuilder()
                        .setRc(
                            Servant.SlotCommandStatus.RC.newBuilder()
                                .setCodeValue(1)
                                .setDescription("Snapshot service was not initialized. Operation is not available.")
                                .build()
                        ).build();
                }
                final Servant.SnapshotCommand snapshot = request.getSnapshot();
                slotConnectionManager.snapshooter().registerSlot(slot, snapshot.getSnapshotId(), snapshot.getEntryId());
                break;
            }

            case CONNECT: {
                final Servant.ConnectSlotCommand connect = request.getConnect();
                final URI slotUri = URI.create(connect.getSlotUri());
                if (slot instanceof LzyInputSlot) {
                    if (SlotS3.match(slotUri) || SlotAzure.match(slotUri)) {
                        ((LzyInputSlot) slot).connect(slotUri, slotConnectionManager.connectToS3(slotUri, 0));
                    } else {
                        ((LzyInputSlot) slot).connect(slotUri, slotConnectionManager.connectToSlot(slotUri, 0));
                    }
                } else {
                    return Servant.SlotCommandStatus.newBuilder()
                        .setRc(Servant.SlotCommandStatus.RC.newBuilder()
                            .setCodeValue(1)
                            .setDescription("Slot " + request.getSlot() + " not found in " + request.getTid())
                            .build())
                        .build();
                }
                break;
            }

            case DISCONNECT: {
                slot.suspend();
                break;
            }

            case STATUS: {
                final Operations.SlotStatus.Builder status = Operations.SlotStatus.newBuilder(slot.status());
                if (auth.hasUser()) {
                    status.setUser(auth.getUser().getUserId());
                }
                return Servant.SlotCommandStatus.newBuilder()
                    .setStatus(status.build())
                    .build();
            }

            case DESTROY: {
                slot.destroy();
                fs.removeSlot(slot.name());
                LOG.info("Explicitly closing slot tid: " + slotName);
                break;
            }

            default:
                throw Status.INVALID_ARGUMENT.asException();
        }

        return Servant.SlotCommandStatus.newBuilder().build();
    }

    public void openOutputSlot(Servant.SlotRequest request, StreamObserver<Servant.Message> responseObserver) {
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
        if (!(slot instanceof LzyOutputSlot)) {
            LOG.info("Trying to read from input slot " + path);
            responseObserver
                .onError(Status.NOT_FOUND.withDescription("Reading from input slot: " + path).asException());
            return;
        }

        final LzyOutputSlot outputSlot = (LzyOutputSlot) slot;
        try {
            outputSlot.readFromPosition(request.getOffset())
                .forEach(chunk -> responseObserver.onNext(Servant.Message.newBuilder().setChunk(chunk).build()));
            responseObserver.onNext(Servant.Message.newBuilder().setControl(Servant.Message.Controls.EOS).build());
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
        LOG.info("Registering command `{}`...", cmd);

        boolean added = fs.addScript(new LzyScriptImpl(cmd, script, zygote), /* isSystem */ zygote == null);

        if (added) {
            LOG.info("Command `{}` registered.", cmd);
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

        return fs;
    }

    private void registerBuiltinCommand(Path cmd, String name, String... args) {
        final String logConfFile = System.getProperty("cmd.log4j.configurationFile");
        final List<String> commandParts = new ArrayList<>();
        commandParts.add(System.getProperty("java.home") + "/bin/java");
        commandParts.add("-Xmx1g");
        commandParts.add("-Dcustom.log.file=" + LOGS_DIR + "/" + name + "_$(($RANDOM % 10000))");
        if (logConfFile != null) {
            commandParts.add("-Dlog4j.configurationFile=" + logConfFile);
        }
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
            LOG.info("Register command `{}`.", name);
        } else {
            LOG.warn("Command `{}` already exists.", name);
        }
    }


    private static final class LzyScriptImpl implements LzyScript {
        private final Path location;
        private final CharSequence scriptText;
        private final Operations.Zygote zygote;

        private LzyScriptImpl(Path location, CharSequence scriptText, Operations.Zygote zygote) {
            this.location = location;
            this.scriptText = scriptText;
            this.zygote = zygote;
        }

        @Override
        public Zygote operation() {
            return GrpcConverter.from(zygote);
        }

        @Override
        public Path location() {
            return location;
        }

        @Override
        public CharSequence scriptText() {
            return scriptText;
        }
    }


    private final class Impl extends LzyFsGrpc.LzyFsImplBase {
        @Override
        public void registerCommand(LzyFsApi.RegisterCommandRequest request,
                                    StreamObserver<LzyFsApi.RegisterCommandResponse> responseObserver) {
            boolean added = LzyFsServer.this.registerCommand(
                Path.of(request.getPath()), request.getCommand(), request.getZygote());

            responseObserver.onNext(
                LzyFsApi.RegisterCommandResponse.newBuilder()
                    .setErrno(added ? 0 : ErrorCodes.EEXIST())
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void unregisterCommand(LzyFsApi.UnregisterCommandRequest request,
                                      StreamObserver<LzyFsApi.UnregisterCommandResponse> responseObserver) {
            responseObserver.onNext(
                LzyFsApi.UnregisterCommandResponse.newBuilder()
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void configureSlot(Servant.SlotCommand request,
                                  StreamObserver<Servant.SlotCommandStatus> responseObserver) {
            try {
                final Servant.SlotCommandStatus slotCommandStatus = LzyFsServer.this.configureSlot(request);
                responseObserver.onNext(slotCommandStatus);
                responseObserver.onCompleted();
            } catch (StatusException e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void openOutputSlot(Servant.SlotRequest request, StreamObserver<Servant.Message> responseObserver) {
            LzyFsServer.this.openOutputSlot(request, responseObserver);
        }
    }
}
