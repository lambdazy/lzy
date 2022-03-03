package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
import ru.yandex.cloud.ml.platform.lzy.servant.BashApi;
import ru.yandex.cloud.ml.platform.lzy.servant.commands.LzyCommand;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFSManager;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyInputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyLinuxFsManagerImpl;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyMacosFsManagerImpl;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyScript;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.SlotConnectionManager;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.SlotConnectionManager.SlotController;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

public abstract class LzyAgent implements Closeable {

    private static final Logger LOG = LogManager.getLogger(LzyAgent.class);
    private static final String LOG_DIR = "/var/log/";
    protected final URI serverAddress;
    protected final Path mount;
    protected final IAM.Auth auth;
    protected final LzyFSManager lzyFS;
    protected final URI agentAddress;
    protected final URI agentInternalAddress;
    protected final AtomicReference<AgentStatus> status = new AtomicReference<>(
        AgentStatus.STARTED);
    protected final SlotConnectionManager slotConnectionManager = new SlotConnectionManager();
    protected final AtomicBoolean inContext = new AtomicBoolean(false);
    protected LzyContext context;

    protected LzyAgent(LzyAgentConfig config) throws URISyntaxException {
        final long start = System.currentTimeMillis();
        this.mount = config.getRoot();
        this.serverAddress = config.getServerAddress();
        if (SystemUtils.IS_OS_MAC) {
            this.lzyFS = new LzyMacosFsManagerImpl();
        } else {
            this.lzyFS = new LzyLinuxFsManagerImpl();
        }
        LOG.info("Mounting LZY FS: " + mount);
        this.lzyFS.mount(mount);
        //this.lzyFS.mount(mount, false, true);

        auth = getAuth(config);
        agentAddress = new URI("http", null, config.getAgentName(), config.getAgentPort(), null,
            null, null);
        agentInternalAddress =
            config.getAgentInternalName() == null ? agentAddress : new URI("http", null,
                config.getAgentInternalName(),
                config.getAgentPort(), null, null, null
            );
        final long finish = System.currentTimeMillis();
        MetricEventLogger.log(
            new MetricEvent(
                "LzyAgent construct time",
                Map.of(
                    "agent_type", this.getClass().getSimpleName(),
                    "metric_type", "system_metric"
                ),
                finish - start
            )
        );
    }

    private static IAM.Auth getAuth(LzyAgentConfig config) {
        final IAM.Auth.Builder authBuilder = IAM.Auth.newBuilder();
        if (config.getUser() != null) {
            final String signedToken = config.getToken();
            final IAM.UserCredentials.Builder credBuilder = IAM.UserCredentials.newBuilder()
                .setUserId(config.getUser())
                .setToken(signedToken);
            authBuilder.setUser(credBuilder.build());
        } else {
            authBuilder.setTask(IAM.TaskCredentials.newBuilder()
                .setTaskId(config.getTask())
                .setToken(config.getToken())
                .build()
            );
        }
        return authBuilder.build();
    }

    protected abstract void onStartUp();

    protected abstract Server server();

    protected abstract LzyServerApi lzyServerApi();

    public void start() throws IOException {
        final Server agentServer = server();
        agentServer.start();

        for (LzyCommand.Commands command : LzyCommand.Commands.values()) {
            publishTool(null, Paths.get(command.name()), command.name());
        }
        final Operations.ZygoteList zygotes = lzyServerApi().zygotes(auth);
        for (Operations.RegisteredZygote zygote : zygotes.getZygoteList()) {
            publishTool(
                zygote.getWorkload(),
                Paths.get(zygote.getName()),
                "run"
            );
        }
        onStartUp();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown hook in lzy-agent {}", agentAddress);
            agentServer.shutdown();
            try {
                close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
    }

    public void awaitTermination() throws InterruptedException {
        server().awaitTermination();
    }

    @Override
    public void close() {
        lzyFS.umount();
    }

    public void publishTool(Operations.Zygote z, Path to, String... servantArgs) {
        LOG.info("publish tool " + z + " to " + to);
        try {
            final String zygoteJson = z != null ? JsonFormat.printer().print(z) : null;
            final String logConfFile = System.getProperty("log4j.configurationFile");
            final List<String> commandParts = new ArrayList<>();
            commandParts.add(System.getProperty("java.home") + "/bin/java");
            commandParts.add("-Xmx1g");
            commandParts.add("-Dcustom.log.file=" + LOG_DIR + to.getFileName() + "_$(($RANDOM % 10000))");
            if (logConfFile != null) {
                commandParts.add("-Dlog4j.configurationFile=" + logConfFile);
            }
            commandParts.add("-classpath");
            commandParts.add('"' + System.getProperty("java.class.path") + '"');
            commandParts.add(BashApi.class.getCanonicalName());
            commandParts.addAll(List.of("--port", Integer.toString(agentAddress.getPort())));
            commandParts.addAll(List.of("--lzy-address", serverAddress.toString()));
            commandParts.addAll(List.of("--lzy-mount", mount.toAbsolutePath().toString()));
            commandParts.addAll(List.of(
                "--auth",
                new String(Base64.getEncoder().encode(auth.toByteString().toByteArray()))
            ));
            commandParts.addAll(Arrays.asList(servantArgs));
            commandParts.add("$@");

            final StringBuilder scriptBuilder = new StringBuilder();
            if (zygoteJson != null) {
                scriptBuilder.append("export ZYGOTE=")
                    .append('"')
                    .append(zygoteJson
                        .replaceAll("\"", "\\\\\"")
                        .replaceAll("\\R", "\\\\\n")
                    )
                    .append('"')
                    .append("\n\n");
            }
            scriptBuilder.append(String.join(" ", commandParts)).append("\n");
            final String script = scriptBuilder.toString();
            lzyFS.addScript(new LzyScript() {
                @Override
                public Zygote operation() {
                    return GrpcConverter.from(z);
                }

                @Override
                public Path location() {
                    return to;
                }

                @Override
                public CharSequence scriptText() {
                    return script;
                }
            }, z == null);

        } catch (InvalidProtocolBufferException ignore) {
            // Ignored
        }
    }

    public void configureSlot(
        Servant.SlotCommand request,
        StreamObserver<Servant.SlotCommandStatus> responseObserver
    ) {
        try {
            final Servant.SlotCommandStatus slotCommandStatus = configureSlot(request);
            responseObserver.onNext(slotCommandStatus);
            responseObserver.onCompleted();
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    public Servant.SlotCommandStatus configureSlot(
        Servant.SlotCommand request
    ) throws StatusException {
        LOG.info("Agent::configureSlot " + JsonUtils.printRequest(request));
        final LzySlot slot = context.slot(request.getSlot()); // null for create
        if (slot == null && request.getCommandCase() != Servant.SlotCommand.CommandCase.CREATE) {
            return Servant.SlotCommandStatus.newBuilder()
                .setRc(
                    Servant.SlotCommandStatus.RC.newBuilder()
                        .setCodeValue(1)
                        .setDescription(
                            "Slot " + request.getSlot() + " is not found in LzyContext")
                        .build()
                ).build();
        }
        switch (request.getCommandCase()) {
            case CREATE:
                final Servant.CreateSlotCommand create = request.getCreate();
                final Slot slotSpec = GrpcConverter.from(create.getSlot());
                final LzySlot lzySlot = context.configureSlot(
                    slotSpec,
                    create.getChannelId()
                );
                if (lzySlot instanceof LzyFileSlot) {
                    LOG.info("lzyFS::addSlot " + lzySlot.name());
                    lzyFS.addSlot((LzyFileSlot) lzySlot);
                    LOG.info("lzyFS:: slot added " + lzySlot.name());
                }
                break;
            case SNAPSHOT:
                final Servant.SnapshotCommand snapshot = request.getSnapshot();
                slot.snapshot(snapshot.getSnapshotId(), snapshot.getEntryId());
                break;
            case CONNECT:
                final Servant.ConnectSlotCommand connect = request.getConnect();
                final URI slotUri = URI.create(connect.getSlotUri());
                final SlotController slotController = slotConnectionManager
                    .getOrCreate(slot.name(), slotUri, LzyServantGrpc.SERVICE_NAME, channel -> {
                        final LzyServantGrpc.LzyServantBlockingStub stub = LzyServantGrpc
                            .newBlockingStub(channel);
                        return stub::openOutputSlot;
                    });
                ((LzyInputSlot) slot).connect(slotUri, slotController);
                break;
            case DISCONNECT:
                if (slot instanceof LzyInputSlot) {
                    slotConnectionManager.shutdownConnections(slot.name());
                }
                slot.suspend();
                break;
            case STATUS:
                final Operations.SlotStatus.Builder status = Operations.SlotStatus
                    .newBuilder(slot.status());
                if (auth.hasUser()) {
                    status.setUser(auth.getUser().getUserId());
                }
                return Servant.SlotCommandStatus.newBuilder().setStatus(status.build()).build();
            case DESTROY:
                slot.destroy();
                lzyFS.removeSlot(slot.name());
                LOG.info("Agent::Explicitly closing slot " + slot.name());
                break;
            default:
                throw Status.INVALID_ARGUMENT.asException();
        }
        return Servant.SlotCommandStatus.newBuilder().build();
    }

    public void update(IAM.Auth request,
                       StreamObserver<Servant.ExecutionStarted> responseObserver) {
        final Operations.ZygoteList zygotes = lzyServerApi().zygotes(auth);
        for (Operations.RegisteredZygote zygote : zygotes.getZygoteList()) {
            publishTool(zygote.getWorkload(), Paths.get(zygote.getName()), "run", zygote.getName());
        }
        responseObserver.onNext(Servant.ExecutionStarted.newBuilder().build());
        responseObserver.onCompleted();
    }

    public void status(IAM.Empty request,
                       StreamObserver<Servant.ServantStatus> responseObserver) {
        final Servant.ServantStatus.Builder builder = Servant.ServantStatus.newBuilder();
        builder.setStatus(status.get().toGrpcServantStatus());
        if (inContext.get()) {
            builder.addAllConnections(context.slots().map(slot -> {
                final Operations.SlotStatus.Builder status = Operations.SlotStatus
                    .newBuilder(slot.status());
                if (auth.hasUser()) {
                    status.setUser(auth.getUser().getUserId());
                }
                return status.build();
            }).collect(Collectors.toList()));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    public interface LzyServerApi {

        Operations.ZygoteList zygotes(IAM.Auth auth);
    }
}
