package ai.lzy.fs;

import ai.lzy.fs.commands.builtin.Cat;
import ai.lzy.fs.fs.LzyFSManager;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzyLinuxFsManagerImpl;
import ai.lzy.fs.fs.LzyMacosFsManagerImpl;
import ai.lzy.fs.fs.LzyScript;
import ai.lzy.iam.grpc.client.AccessServiceGrpcClient;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.grpc.interceptors.CheckAccessInterceptor;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import jakarta.inject.Named;
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
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;
import static io.grpc.ServerInterceptors.intercept;

public class LzyFsServer {
    private static final Logger LOG = LogManager.getLogger(LzyFsServer.class);
    public static final AtomicInteger mounted = new AtomicInteger(); //for tests

    private final Path mountPoint;
    private final HostAndPort selfService;
    private final SlotsManager slotsManager;
    private final LzyFSManager fsManager;
    private final SlotsService slotsService;
    private final CheckAccessInterceptor accessInterceptor;
    private final Server localServer;
    private final AtomicBoolean finished = new AtomicBoolean(false);

    public LzyFsServer(String agentId, Path mountPoint, HostAndPort selfAddress,
                       @Named("WorkerChannelManagerGrpcChannel") ManagedChannel channelManagerChannel,
                       @Named("WorkerIamGrpcChannel") ManagedChannel iamChannel,
                       RenewableJwt token, LocalOperationService operationService, boolean isPortal)
    {
        this.mountPoint = mountPoint;
        this.selfService = selfAddress;

        final var channelManagerClient = newBlockingClient(
            LzyChannelManagerGrpc.newBlockingStub(channelManagerChannel), "LzyFs", () -> token.get().token());

        final var channelManagerOperationClient = newBlockingClient(
            LongRunningServiceGrpc.newBlockingStub(channelManagerChannel), "LzyFs", () -> token.get().token());

        this.slotsManager = new SlotsManager(channelManagerClient, channelManagerOperationClient,
            selfAddress, isPortal);

        if (SystemUtils.IS_OS_MAC) {
            this.fsManager = new LzyMacosFsManagerImpl();
        } else if (SystemUtils.IS_OS_LINUX) {
            this.fsManager = new LzyLinuxFsManagerImpl();
        } else {
            throw new RuntimeException(SystemUtils.OS_NAME + " is not supported");
        }

        this.slotsService = new SlotsService(agentId, operationService, slotsManager, fsManager, token);

        var authClient = new AuthenticateServiceGrpcClient(agentId, iamChannel);
        var authInterceptor = new AuthServerInterceptor(authClient);


        var accessClient = new AccessServiceGrpcClient(agentId, iamChannel);
        var internalOnlyInterceptor = new AllowInternalUserOnlyInterceptor(accessClient);
        accessInterceptor = new CheckAccessInterceptor(accessClient);

        this.localServer = newGrpcServer(selfAddress.getHost(), selfAddress.getPort(), authInterceptor)
            .addService(
                intercept(
                    slotsService.getSlotsApi(),
                    accessInterceptor,
                    internalOnlyInterceptor.ignoreMethods(LzySlotsApiGrpc.getOpenOutputSlotMethod())))
            .addService(
                intercept(
                    slotsService.getLongrunningApi(),
                    accessInterceptor,
                    internalOnlyInterceptor))
            .build();
    }

    public void start() throws IOException {
        LOG.info("Mounting LzyFs at {}.", mountPoint.toAbsolutePath().toString());
        try {
            fsManager.mount(mountPoint);
            mounted.incrementAndGet();
        } catch (FuseException e) {
            fsManager.umount();
            throw new IOException("Cannot mount %s".formatted(mountPoint.toAbsolutePath()), e);
        }

        LOG.info("Starting LzyFs gRPC server at {}.", selfService);
        localServer.start();

        LOG.info("Registering lzy cat command...");
        registerCatCommand();
        LOG.info("LzyFs started on {}.", selfService);
    }

    public void configureAccess(AuthResource authResource, AuthPermission authPermission) {
        accessInterceptor.configure(authResource, authPermission);
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
    ) implements LzyScript {}

    public void stop() {
        LOG.info("LzyFs shutdown request at {}, path {}", selfService, mountPoint);
        if (finished.compareAndSet(false, true)) {
            try {
                slotsService.shutdown();
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
        LOG.info("LzyFs awaiting termination at {}.", selfService);
        try {
            slotsManager.close();
        } finally {
            stop();
        }
        LOG.info("LzyFs at {} terminated.", selfService);
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
