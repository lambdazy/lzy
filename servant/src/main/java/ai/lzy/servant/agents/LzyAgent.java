package ai.lzy.servant.agents;

import ai.lzy.fs.LzyFsServerLegacy;
import ai.lzy.logs.MetricEvent;
import ai.lzy.logs.MetricEventLogger;
import ai.lzy.servant.BashApi;
import ai.lzy.servant.commands.ServantCommandHolder;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.deprecated.LzyAuth;
import ai.lzy.v1.deprecated.LzyZygote;
import ai.lzy.v1.deprecated.Servant;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
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
import java.util.stream.Collectors;

import static ai.lzy.model.Constants.LOGS_DIR;
import static ai.lzy.model.UriScheme.LzyFs;

public class LzyAgent implements Closeable {
    private static final Logger LOG = LogManager.getLogger(LzyAgent.class);
    private static final ThreadGroup SHUTDOWN_HOOK_TG = new ThreadGroup("shutdown-hooks");

    private final LzyAgentConfig config;
    private final LzyAuth.Auth auth;
    private final AtomicReference<AgentStatus> status = new AtomicReference<>(AgentStatus.STARTED);
    private final URI serverUri;
    private final Server server;
    private final LzyFsServerLegacy lzyFs;
    private final LzyContext context;

    protected LzyAgent(LzyAgentConfig config, String agentName, BindableService... agentServices)
        throws URISyntaxException, IOException {
        final long start = System.currentTimeMillis();

        this.config = config;
        this.auth = getAuth(config);

        LOG.info("Starting agent {} at {}://{}:{}/{} with fs at {}:{}",
            agentName,
            config.getScheme(),
            config.getAgentHost(),
            config.getAgentPort(),
            config.getRoot(),
            config.getAgentHost(),
            config.getFsPort());

        serverUri = new URI(config.getScheme(), null, config.getAgentHost(), config.getAgentPort(), null, null, null);

        final NettyServerBuilder nettyServerBuilder = NettyServerBuilder
            .forAddress(new InetSocketAddress(config.getAgentHost(), config.getAgentPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES);
        for (var service: agentServices) {
            nettyServerBuilder.addService(service);
        }
        server = nettyServerBuilder.build();
        server.start();

        lzyFs = new LzyFsServerLegacy(
            config.getAgentId(),
            config.getRoot().toString(),
            new URI(LzyFs.scheme(), null, config.getAgentHost(), config.getFsPort(), null, null, null),
            config.getServerAddress(),
            config.getWhiteboardAddress(),
            config.getChannelManagerAddress(),
            auth);
        lzyFs.start();
        context = new LzyContext(config.getAgentId(),
            lzyFs.getSlotsManager(),
            lzyFs.getMountPoint().toString());
        for (ServantCommandHolder command : ServantCommandHolder.values()) {
            publishTool(null, Paths.get(command.name()), command.name());
        }

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

    public void publishTools(LzyZygote.ZygoteList zygotes) {
        for (LzyZygote.Zygote zygote : zygotes.getZygoteList()) {
            publishTool(
                zygote,
                Paths.get(zygote.getName()),
                "run"
            );
        }
    }

    private static LzyAuth.Auth getAuth(LzyAgentConfig config) {
        final LzyAuth.Auth.Builder authBuilder = LzyAuth.Auth.newBuilder();
        if (config.getUser() != null) {
            final String signedToken = config.getToken();
            final LzyAuth.UserCredentials.Builder credBuilder = LzyAuth.UserCredentials.newBuilder()
                .setUserId(config.getUser())
                .setToken(signedToken);
            authBuilder.setUser(credBuilder.build());
        } else {
            authBuilder.setTask(LzyAuth.TaskCredentials.newBuilder()
                .setServantId(config.getAgentId())
                .setServantToken(config.getToken())
                .build()
            );
        }
        return authBuilder.build();
    }

    public void awaitTermination() throws InterruptedException, IOException {
        server.awaitTermination();
    }

    @Override
    public void close() {
        LOG.info("LzyAgent::close {}", serverUri);
        try {
            lzyFs.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void publishTool(LzyZygote.Zygote z, Path to, String... servantArgs) {
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

    public void update(LzyZygote.ZygoteList zygotes, StreamObserver<LzyAuth.Empty> responseObserver) {
        for (LzyZygote.Zygote zygote : zygotes.getZygoteList()) {
            publishTool(zygote, Paths.get(zygote.getName()), "run", zygote.getName());
        }
        responseObserver.onNext(LzyAuth.Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    public void status(@SuppressWarnings("unused") LzyAuth.Empty request,
                       StreamObserver<Servant.ServantStatus> responseObserver) {
        final Servant.ServantStatus.Builder builder = Servant.ServantStatus.newBuilder();
        builder.setStatus(status.get().toGrpcServantStatus());
        builder.addAllConnections(context.slots().map(slot -> {
            final LMS.SlotStatus.Builder status = LMS.SlotStatus
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

    public LzyAuth.Auth auth() {
        return auth;
    }

    public void updateStatus(AgentStatus newStatus) {
        AgentStatus oldValue;
        do {
            oldValue = status.get();
        } while (!status.compareAndSet(oldValue, newStatus));
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

    public LzyFsServerLegacy fs() {
        return lzyFs;
    }

    public void shutdown() {
        server.shutdown();
    }

    public void shutdownNow() {
        server.shutdownNow();
    }
}
