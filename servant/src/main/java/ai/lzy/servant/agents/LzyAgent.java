package ai.lzy.servant.agents;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.fs.LzyFsServer;
import ai.lzy.model.logs.MetricEvent;
import ai.lzy.model.logs.MetricEventLogger;
import ai.lzy.servant.BashApi;
import ai.lzy.servant.commands.ServantCommandHolder;
import ai.lzy.priv.v2.IAM;
import ai.lzy.priv.v2.LzyServerGrpc;
import ai.lzy.priv.v2.Operations;
import ai.lzy.priv.v2.Servant;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static ai.lzy.model.Constants.LOGS_DIR;
import static ai.lzy.model.UriScheme.LzyFs;

public abstract class LzyAgent implements Closeable {
    private static final Logger LOG = LogManager.getLogger(LzyAgent.class);
    private static final ThreadGroup SHUTDOWN_HOOK_TG = new ThreadGroup("shutdown-hooks");

    protected final LzyAgentConfig config;
    protected final IAM.Auth auth;
    protected final LzyFsServer lzyFs;
    protected final AtomicReference<AgentStatus> status = new AtomicReference<>(AgentStatus.STARTED);

    protected LzyAgent(LzyAgentConfig config) throws URISyntaxException, IOException {
        final long start = System.currentTimeMillis();

        this.config = config;
        this.auth = getAuth(config);

        this.lzyFs = new LzyFsServer(
            config.getServantId(),
            config.getRoot().toString(),
            new URI(LzyFs.scheme(), null, config.getAgentHost(), config.getFsPort(), null, null, null),
            config.getServerAddress(),
            config.getWhiteboardAddress(),
            auth);

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
                .setServantId(config.getServantId())
                .setServantToken(config.getToken())
                .build()
            );
        }
        return authBuilder.build();
    }

    protected abstract LzyContext context();

    protected abstract void onStartUp();

    protected void started() {
    }

    protected abstract URI serverUri();

    protected abstract Server server();

    protected abstract LzyServerGrpc.LzyServerBlockingStub serverApi();

    public void start() throws IOException {
        final URI agentUri = serverUri();
        final Server agentServer = server();
        agentServer.start();

        onStartUp();

        for (ServantCommandHolder command : ServantCommandHolder.values()) {
            publishTool(null, Paths.get(command.name()), command.name());
        }
        final Operations.ZygoteList zygotes = serverApi().zygotes(auth);
        for (Operations.Zygote zygote : zygotes.getZygoteList()) {
            publishTool(
                zygote,
                Paths.get(zygote.getName()),
                "run"
            );
        }

        Runtime.getRuntime().addShutdownHook(new Thread(SHUTDOWN_HOOK_TG, () -> {
            LOG.info("Shutdown hook in lzy-agent {}", agentUri);
            agentServer.shutdown();
            try {
                close();
            } catch (Exception e) {
                LOG.error(e);
                throw new RuntimeException(e);
            }
        }, "agent-shutdown-hook"));
        started();
    }

    public void awaitTermination() throws InterruptedException {
        server().awaitTermination();
        try {
            lzyFs.awaitTermination();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        LOG.info("LzyAgent::close {}", serverUri());
        try {
            lzyFs.stop();
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
            commandParts.addAll(List.of("--port", Integer.toString(serverUri().getPort())));
            commandParts.addAll(List.of("--fs-port", Integer.toString(lzyFs.getUri().getPort())));
            commandParts.addAll(List.of("--lzy-address", config.getServerAddress().toString()));
            commandParts.addAll(List.of("--lzy-whiteboard", config.getWhiteboardAddress().toString()));
            commandParts.addAll(List.of("--lzy-mount", lzyFs.getMountPoint().toAbsolutePath().toString()));
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
                LOG.info("Command `{}` registered.", to);
            } else {
                LOG.warn("Command `{}` already exists.", to);
            }
        } catch (InvalidProtocolBufferException ignore) {
            // Ignored
        }
    }

    public void update(@SuppressWarnings("unused") IAM.Auth request, StreamObserver<IAM.Empty> responseObserver) {
        final Operations.ZygoteList zygotes = serverApi().zygotes(auth);
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
        builder.addAllConnections(context().slots().map(slot -> {
            final Operations.SlotStatus.Builder status = Operations.SlotStatus
                .newBuilder(slot.status());
            if (auth.hasUser()) {
                status.setUser(auth.getUser().getUserId());
            }
            return status.build();
        }).collect(Collectors.toList()));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }
}
