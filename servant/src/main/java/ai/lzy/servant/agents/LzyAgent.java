package ai.lzy.servant.agents;

import static ai.lzy.model.Constants.LOGS_DIR;
import static ai.lzy.model.UriScheme.LzyFs;

import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.storage.StorageClient;
import ai.lzy.model.exceptions.EnvironmentInstallationException;
import ai.lzy.model.graph.AtomicZygote;
import ai.lzy.model.graph.Env;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.logs.MetricEvent;
import ai.lzy.model.logs.MetricEventLogger;
import ai.lzy.v1.IAM;
import ai.lzy.v1.Lzy;
import ai.lzy.v1.LzyServantGrpc;
import ai.lzy.v1.LzyServerGrpc;
import ai.lzy.v1.Operations;
import ai.lzy.v1.Servant;
import ai.lzy.servant.BashApi;
import ai.lzy.servant.commands.ServantCommandHolder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LzyAgent implements Closeable {
    private static final Logger LOG = LogManager.getLogger(LzyAgent.class);
    private static final ThreadGroup SHUTDOWN_HOOK_TG = new ThreadGroup("shutdown-hooks");

    private final LzyAgentConfig config;
    private final IAM.Auth auth;
    private final AtomicReference<AgentStatus> status = new AtomicReference<>(AgentStatus.STARTED);
    private final URI serverUri;
    private final Server server;
    private final LzyFsServer lzyFs;
    private final LzyContext context;

    protected LzyAgent(LzyAgentConfig config, LzyServantGrpc.LzyServantImplBase servantImpl)
        throws URISyntaxException, IOException {
        final long start = System.currentTimeMillis();

        this.config = config;
        this.auth = getAuth(config);

        LOG.info("Starting agent {} at {}://{}:{}/{} with fs at {}:{}",
            servantImpl.getClass().getName(),
            config.getScheme(),
            config.getAgentHost(),
            config.getAgentPort(),
            config.getRoot(),
            config.getAgentHost(),
            config.getFsPort());

        serverUri = new URI(config.getScheme(), null, config.getAgentHost(), config.getAgentPort(), null, null, null);

        server = NettyServerBuilder
            .forAddress(new InetSocketAddress(config.getAgentHost(), config.getAgentPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(servantImpl)
            .build();
        server.start();

        status.set(AgentStatus.PREPARING_EXECUTION);

        lzyFs = new LzyFsServer(
            config.getAgentId(),
            config.getRoot().toString(),
            new URI(LzyFs.scheme(), null, config.getAgentHost(), config.getFsPort(), null, null, null),
            config.getServerAddress(),
            config.getWhiteboardAddress(),
            config.getChannelManagerAddress(),
            auth);
        context = new LzyContext(config.getAgentId(),
            lzyFs.getSlotsManager(),
            lzyFs.getMountPoint().toString());
        status.set(AgentStatus.EXECUTING);

        Runtime.getRuntime().addShutdownHook(new Thread(SHUTDOWN_HOOK_TG, () -> {
            LOG.info("Shutdown hook in lzy-agent {}", serverUri);
            server.shutdown();
            try {
                close();
            } catch (Exception e) {
                LOG.error(e);
                throw new RuntimeException(e);
            }
        }, "agent-shutdown-hook"));

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

    public void publishTools(Operations.ZygoteList zygotes) {
        for (ServantCommandHolder command : ServantCommandHolder.values()) {
            publishTool(null, Paths.get(command.name()), command.name());
        }
        for (Operations.Zygote zygote : zygotes.getZygoteList()) {
            publishTool(
                zygote,
                Paths.get(zygote.getName()),
                "run"
            );
        }
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
                .setServantId(config.getAgentId())
                .setServantToken(config.getToken())
                .build()
            );
        }
        return authBuilder.build();
    }

    public void awaitTermination() throws InterruptedException, IOException {
        server.awaitTermination();
        lzyFs.awaitTermination();
    }

    public void closeSlots() {
        LOG.info("LzyAgent::closeSlots");
        context.slots().forEach(slot -> {
            LOG.info("  suspending slot {} ({})...", slot.name(), slot.status().getState());
            slot.suspend();
        });
    }

    @Override
    public void close() {
        LOG.info("LzyAgent::close {}", serverUri);
        try {
            context.close();
            lzyFs.stop();
            server.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void publishTool(Operations.Zygote z, Path to, String... servantArgs) {
        if (z != null)
            LOG.info("published zygote " + to);
        try {
            final String zygoteJson = z != null ? JsonFormat.printer().print(z) : null;
            final List<String> commandParts = new ArrayList<>();
            commandParts.add(System.getProperty("java.home") + "/bin/java");
            commandParts.add("-Xmx1g");
            commandParts.add("-Dcustom.log.file=" + LOGS_DIR + to.getFileName() + "_$(($RANDOM % 10000))");
            commandParts.add("-Dlog4j.configurationFile=servant_cmd/log4j2.yaml");
            commandParts.add("-classpath");
            commandParts.add('"' + System.getProperty("java.class.path") + '"');
            commandParts.add(BashApi.class.getCanonicalName());
            commandParts.addAll(List.of("--port", Integer.toString(serverUri.getPort())));
            commandParts.addAll(List.of("--fs-port", Integer.toString(lzyFs.getUri().getPort())));
            commandParts.addAll(List.of("--lzy-address", config.getServerAddress().toString()));
            commandParts.addAll(List.of("--lzy-whiteboard", config.getWhiteboardAddress().toString()));
            commandParts.addAll(List.of("--channel-manager", config.getChannelManagerAddress().toString()));
            commandParts.addAll(List.of("--lzy-mount", lzyFs.getMountPoint().toAbsolutePath().toString()));
            commandParts.addAll(List.of("--agent-id", config.getAgentId()));
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

            if (lzyFs.registerCommand(to, script, z)) {
                LOG.debug("Command `{}` registered.", to);
            } else {
                LOG.warn("Command `{}` already exists.", to);
            }
        } catch (InvalidProtocolBufferException ignore) {
            // Ignored
        }
    }

    public void update(Operations.ZygoteList zygotes, StreamObserver<IAM.Empty> responseObserver) {
        for (Operations.Zygote zygote : zygotes.getZygoteList()) {
            publishTool(zygote, Paths.get(zygote.getName()), "run", zygote.getName());
        }
        responseObserver.onNext(IAM.Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    public void status(@SuppressWarnings("unused") IAM.Empty request,
                       StreamObserver<Servant.ServantStatus> responseObserver) {
        final Servant.ServantStatus.Builder builder = Servant.ServantStatus.newBuilder();
        builder.setStatus(status.get().toGrpcServantStatus());
        builder.addAllConnections(context.slots().map(slot -> {
            final Operations.SlotStatus.Builder status = Operations.SlotStatus
                .newBuilder(slot.status());
            return status.build();
        }).collect(Collectors.toList()));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    public URI uri() {
        return serverUri;
    }

    public URI fsUri() {
        return lzyFs.getUri();
    }

    public IAM.Auth auth() {
        return auth;
    }

    public void updateStatus(AgentStatus newStatus) {
        AgentStatus oldValue;
        do {
            oldValue = status.get();
        } while (!status.compareAndSet(oldValue, newStatus));
    }

    public void forceStop(String reason, Throwable th) {
        LOG.error("Force terminate servant {}: {}", config.getAgentId(), reason, th);
        server.shutdownNow();
        lzyFs.forceStop();
    }

    public void register(LzyServerGrpc.LzyServerBlockingStub server) {
        updateStatus(AgentStatus.REGISTERING);
        final Lzy.AttachServant.Builder commandBuilder = Lzy.AttachServant.newBuilder();
        commandBuilder.setAuth(auth());
        commandBuilder.setServantURI(uri().toString());
        commandBuilder.setFsURI(fsUri().toString());
        commandBuilder.setServantId(config.getAgentId());
        //noinspection ResultOfMethodCallIgnored
        server.registerServant(commandBuilder.build());
        updateStatus(AgentStatus.REGISTERED);
    }

    public AgentStatus getStatus() {
        return status.get();
    }

    public String id() {
        return config.getAgentId();
    }

    public LzyContext context() {
        return context;
    }

    public LzyFsServer fs() {
        return lzyFs;
    }

    public void shutdown() {
        lzyFs.stop();
        server.shutdown();
    }
}
