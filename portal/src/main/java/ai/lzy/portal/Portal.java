package ai.lzy.portal;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.SlotsManager;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.model.slot.Slot;
import ai.lzy.portal.config.PortalConfig;
import ai.lzy.portal.slots.SnapshotSlotsProvider;
import ai.lzy.portal.slots.StdoutSlot;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.GrpcHeadersServerInterceptor;
import ai.lzy.util.grpc.GrpcLogsInterceptor;
import ai.lzy.util.grpc.RequestIdInterceptor;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static ai.lzy.model.UriScheme.LzyServant;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class Portal {
    private static final Logger LOG = LogManager.getLogger(Portal.class);

    public static final String APP = "LzyPortal";

    public static final String PORTAL_SLOT_PREFIX = "/portal_slot";

    private final String stdoutChannelId;
    private final String stderrChannelId;

    private final int port;
    private final String host;

    private final ManagedChannel iamChannel;

    // services
    private final Server grpcServer;
    private final LzyFsServer fsServer;
    private final AllocatorAgent allocatorAgent;

    private SlotsManager slotsManager;

    // slots
    private StdoutSlot stdoutSlot;
    private StdoutSlot stderrSlot;
    private final SnapshotSlotsProvider snapshots;

    private final String portalId;

    private CountDownLatch started;

    public Portal(PortalConfig config, AllocatorAgent agent, LzyFsServer fs) {
        this.stdoutChannelId = config.getStdoutChannelId();
        this.stderrChannelId = config.getStderrChannelId();

        this.port = config.getPortalApiPort();
        this.host = config.getHost();

        this.iamChannel = newGrpcChannel(config.getIamAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);

        var internalOnly = new AllowInternalUserOnlyInterceptor(APP, iamChannel);

        this.grpcServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(config.getHost(), config.getPortalApiPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .intercept(new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel)))
            .intercept(GrpcLogsInterceptor.server())
            .intercept(RequestIdInterceptor.server())
            .intercept(GrpcHeadersServerInterceptor.create())
            .addService(ServerInterceptors.intercept(new PortalApiImpl(this), internalOnly))
            .build();

        this.allocatorAgent = agent;
        this.fsServer = fs;

        var prev = fsServer.setSlotApiInterceptor(new FsApiImpl(this));
        assert prev == null;

        this.snapshots = new SnapshotSlotsProvider();
        this.portalId = config.getPortalId();
    }

    public void start() {
        started = new CountDownLatch(1);

        LOG.info("Starting portal with config: { portalId: '{}', url: '{}:/{}:{}', fsRoot: '{}', " +
            "stdoutChannelId: '{}', stderrChannelId: '{}'}", portalId, LzyServant.scheme(), host, port,
            fsServer.getMountPoint(), stdoutChannelId, stderrChannelId);

        try {
            grpcServer.start();
            allocatorAgent.start();
            fsServer.start();
        } catch (IOException | AllocatorAgent.RegisterException e) {
            LOG.error(e);
            this.shutdown();
            throw new RuntimeException(e);
        }

        LOG.info("Registering portal stdout/err slots...");

        var stdoutSlotName = PORTAL_SLOT_PREFIX + ":" + Slot.STDOUT_SUFFIX;
        var stderrSlotName = PORTAL_SLOT_PREFIX + ":" + Slot.STDERR_SUFFIX;

        slotsManager = fsServer.getSlotsManager();
        stdoutSlot = new StdoutSlot(stdoutSlotName, portalId, stdoutChannelId,
            slotsManager.resolveSlotUri(portalId, stdoutSlotName));
        slotsManager.registerSlot(stdoutSlot);

        stderrSlot = new StdoutSlot(stderrSlotName, portalId, stderrChannelId,
            slotsManager.resolveSlotUri(portalId, stderrSlotName));
        slotsManager.registerSlot(stderrSlot);

        LOG.info("Portal successfully started at '{}:{}'", host, port);

        started.countDown();
    }

    public void shutdown() {
        LOG.info("Stopping portal");
        iamChannel.shutdown();
        grpcServer.shutdown();
        allocatorAgent.shutdown();
        fsServer.stop();
    }

    public void shutdownNow() {
        iamChannel.shutdownNow();
        grpcServer.shutdownNow();
        allocatorAgent.shutdown();
        fsServer.stop();
    }

    public void awaitTermination() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean awaitTermination(long count, TimeUnit timeUnit) throws InterruptedException {
        return grpcServer.awaitTermination(count, timeUnit);
    }

    public CountDownLatch started() {
        return started;
    }

    public SlotsManager getSlotManager() {
        return slotsManager;
    }

    public String getPortalId() {
        return portalId;
    }

    public LzyInputSlot findOutSlot(String name) {
        return stdoutSlot.find(name);
    }

    public LzyInputSlot findErrSlot(String name) {
        return stderrSlot.find(name);
    }

    StdoutSlot getStdoutSlot() {
        return stdoutSlot;
    }

    StdoutSlot getStderrSlot() {
        return stderrSlot;
    }

    List<StdoutSlot> getOutErrSlots() {
        if (stdoutSlot != null && stderrSlot != null) {
            return List.of(stdoutSlot, stderrSlot);
        }
        return Collections.emptyList();
    }

    SnapshotSlotsProvider getSnapshots() {
        return snapshots;
    }

    public static class CreateSlotException extends Exception {
        public CreateSlotException(String message) {
            super(message);
        }

        public CreateSlotException(Throwable cause) {
            super(cause);
        }
    }
}
