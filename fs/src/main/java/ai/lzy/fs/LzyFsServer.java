package ai.lzy.fs;

import ai.lzy.fs.commands.BuiltinCommandHolder;
import ai.lzy.fs.fs.LzyFSManager;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzyLinuxFsManagerImpl;
import ai.lzy.fs.fs.LzyMacosFsManagerImpl;
import ai.lzy.fs.fs.LzyScript;
import ai.lzy.model.deprecated.Zygote;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.serce.jnrfuse.FuseException;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.lzy.model.Constants.LOGS_DIR;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;
import static ai.lzy.v1.channel.LzyChannelManagerGrpc.newBlockingStub;


public class LzyFsServer {
    private static final Logger LOG = LogManager.getLogger(LzyFsServer.class);
    public static final AtomicInteger mounted = new AtomicInteger(); //for tests

    private final String agentId;
    private final Path mountPoint;
    private final URI selfUri;
    private final HostAndPort channelManagerAddress;
    private final ManagedChannel channelManagerChannel;
    private final SlotsManager slotsManager;
    private final LzyFSManager fsManager;
    private final SlotsService slotsService;
    private final Server localServer;
    private final AtomicBoolean finished = new AtomicBoolean(false);

    public LzyFsServer(String agentId, Path mountPoint, URI selfUri, HostAndPort channelManagerAddress,
                       RenewableJwt token)
    {
        this.agentId = agentId;
        this.mountPoint = mountPoint;
        this.selfUri = selfUri;
        this.channelManagerAddress = channelManagerAddress;

        this.channelManagerChannel = newGrpcChannel(channelManagerAddress, LzyChannelManagerGrpc.SERVICE_NAME);
        this.slotsManager = new SlotsManager(
            newBlockingClient(
                newBlockingStub(channelManagerChannel),
                "LzyFs",
                () -> token.get().token()),
            selfUri
        );

        if (SystemUtils.IS_OS_MAC) {
            this.fsManager = new LzyMacosFsManagerImpl();
        } else if (SystemUtils.IS_OS_LINUX) {
            this.fsManager = new LzyLinuxFsManagerImpl();
        } else {
            throw new RuntimeException(SystemUtils.OS_NAME + " is not supported");
        }

        this.slotsService = new SlotsService(agentId, slotsManager, fsManager);

        this.localServer = newGrpcServer(selfUri.getHost(), selfUri.getPort(), GrpcUtils.NO_AUTH)
            .addService(slotsService.getSlotsApi())
            .addService(slotsService.getLongrunningApi())
            .build();
    }

    public void start() throws IOException {
        LOG.info("Mounting LzyFs at {}.", mountPoint.toAbsolutePath().toString());
        try {
            fsManager.mount(mountPoint);
            mounted.incrementAndGet();
        } catch (FuseException e) {
            fsManager.umount();
            throw e;
        }

        LOG.info("Starting LzyFs gRPC server at {}.", selfUri);
        localServer.start();

        LOG.info("Registering lzy commands...");
        for (BuiltinCommandHolder command : BuiltinCommandHolder.values()) {
            registerBuiltinCommand(Path.of(command.name()), command.name());
        }

        LOG.info("LzyFs started on {}.", selfUri);
    }

    public void stop() {
        LOG.info("LzyFs shutdown request at {}, path {}", selfUri, mountPoint);
        if (finished.compareAndSet(false, true)) {
            try {
                slotsService.shutdown();
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

    private void registerBuiltinCommand(Path cmd, String name, String... args) {
        final List<String> commandParts = new ArrayList<>();
        commandParts.add(System.getProperty("java.home") + "/bin/java");
        commandParts.add("-Xmx1g");
        commandParts.add("-Dcustom.log.file=" + LOGS_DIR + "/" + name + "_$(($RANDOM % 10000))");
        commandParts.add("-Dlog4j.configurationFile=servant_cmd/log4j2.yaml");
        commandParts.add("-classpath");
        commandParts.add('"' + System.getProperty("java.class.path") + '"');
        commandParts.add(BashApi.class.getCanonicalName());
        commandParts.addAll(List.of("--channel-manager", channelManagerAddress.toString()));
        //commandParts.addAll(List.of("--lzy-whiteboard", whiteboardAddress.toString()));
        commandParts.addAll(List.of("--lzy-mount", mountPoint.toAbsolutePath().toString()));
        commandParts.addAll(List.of("--agent-id", agentId));
        commandParts.addAll(List.of("--lzy-fs-port", Integer.toString(selfUri.getPort())));
        //commandParts.addAll(List.of(
        //    "--auth",
        //    new String(Base64.getEncoder().encode(auth.toByteString().toByteArray()))
        //));
        commandParts.add(name);
        commandParts.addAll(Arrays.asList(args));
        commandParts.add("$@");

        final String script = String.join(" ", commandParts) + "\n";
        if (fsManager.addScript(new LzyScriptImpl(cmd, script), /* isSystem */ true)) {
            LOG.debug("Register command `{}`.", name);
        } else {
            LOG.warn("Command `{}` already exists.", name);
        }
    }

    private record LzyScriptImpl(Path location, String scriptText) implements LzyScript {
        @Override
        public Zygote operation() {
            return null;
        }
    }
}
