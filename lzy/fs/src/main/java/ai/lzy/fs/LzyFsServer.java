package ai.lzy.fs;

import ai.lzy.fs.commands.builtin.Cat;
import ai.lzy.fs.fs.LzyFSManager;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzyLinuxFsManagerImpl;
import ai.lzy.fs.fs.LzyMacosFsManagerImpl;
import ai.lzy.fs.fs.LzyScript;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.serce.jnrfuse.FuseException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

public class LzyFsServer {
    private static final Logger LOG = LogManager.getLogger(LzyFsServer.class);
    public static final AtomicInteger mounted = new AtomicInteger(); //for tests

    private final Path mountPoint;
    private final HostAndPort selfUri;
    private final ManagedChannel channelManagerChannel;
    private final SlotsManager slotsManager;
    private final LzyFSManager fsManager;
    private final SlotsService slotsService;
    private final Server localServer;
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final ManagedChannel iamChannel;

    public LzyFsServer(String agentId, Path mountPoint, HostAndPort selfAddress, HostAndPort channelManagerAddress,
                       HostAndPort iamAddress, RenewableJwt token, LocalOperationService operationService,
                       boolean isPortal)
    {
        this.mountPoint = mountPoint;
        this.selfUri = selfAddress;

        this.channelManagerChannel = newGrpcChannel(channelManagerAddress, LzyChannelManagerGrpc.SERVICE_NAME);
        this.iamChannel = GrpcUtils.newGrpcChannel(iamAddress, LzyAuthenticateServiceGrpc.SERVICE_NAME);

        final var channelManagerClient = newBlockingClient(
            LzyChannelManagerGrpc.newBlockingStub(channelManagerChannel),
            "LzyFs.ChannelManagerClient", () -> token.get().token());

        final var channelManagerOperationClient = newBlockingClient(
            LongRunningServiceGrpc.newBlockingStub(channelManagerChannel),
            "LzyFs.ChannelManagerOperationClient", () -> token.get().token());

        this.slotsManager = new SlotsManager(channelManagerClient, channelManagerOperationClient,
            selfAddress, isPortal);

        if (SystemUtils.IS_OS_MAC) {
            this.fsManager = new LzyMacosFsManagerImpl();
        } else if (SystemUtils.IS_OS_LINUX) {
            this.fsManager = new LzyLinuxFsManagerImpl();
        } else {
            throw new RuntimeException(SystemUtils.OS_NAME + " is not supported");
        }

        this.slotsService = new SlotsService(agentId, operationService, slotsManager, fsManager);

        this.localServer = newGrpcServer(selfAddress.getHost(), selfAddress.getPort(),
            new AuthServerInterceptor(new AuthenticateServiceGrpcClient(agentId, iamChannel)))
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

        LOG.info("Registering lzy cat command...");
        registerCatCommand();
        LOG.info("LzyFs started on {}.", selfUri);
    }

    private void registerCatCommand() {
        var name = "cat";
        var cmd = Path.of(name);
        final List<String> commandParts = new ArrayList<>();
        commandParts.add(System.getProperty("java.home") + "/bin/java");
        commandParts.add("-classpath");
        commandParts.add('"' + System.getProperty("java.class.path") + '"');
        commandParts.add(Cat.class.getCanonicalName());
        commandParts.add("$@");

        final String script = String.join(" ", commandParts) + "\n";
        if (fsManager.addScript(new LzyScriptImpl(cmd, script), /* isSystem */ true)) {
            LOG.debug("Register command `{}`.", name);
        } else {
            LOG.warn("Command `{}` already exists.", name);
        }
    }

    private record LzyScriptImpl(
        Path location,
        CharSequence scriptText
    ) implements LzyScript
    {
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

    public SlotsManager getSlotsManager() {
        return slotsManager;
    }

    public Path getMountPoint() {
        return mountPoint;
    }

    public void addSlot(LzyFileSlot slot) {
        LOG.info("Explicitly add slot: {}", slot.name());
        fsManager.addSlot(slot);
    }
}
