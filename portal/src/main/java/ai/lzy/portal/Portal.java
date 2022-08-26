package ai.lzy.portal;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.SlotsManager;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.model.UriScheme;
import ai.lzy.portal.config.PortalConfig;
import ai.lzy.portal.slots.SnapshotSlotsProvider;
import ai.lzy.portal.slots.StdoutSlot;
import ai.lzy.util.grpc.ChannelBuilder;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static ai.lzy.model.UriScheme.LzyFs;

public class Portal {
    private static final Logger LOG = LogManager.getLogger(Portal.class);

    private static final String stdoutSlotName = "/portal:stdout";
    private static final String stderrSlotName = "/portal:stderr";

    private final String portalTaskId;

    private final PortalConfig config;

    private final Server grpcServer;
    private LzyFsServer fsServer;
    private AllocatorAgent allocatorAgent;

    // stdout/stderr (guarded by this)
    private StdoutSlot stdoutSlot;
    private StdoutSlot stderrSlot;
    private final SnapshotSlotsProvider snapshots;

    public Portal(PortalConfig config) {
        this.config = config;
        this.portalTaskId = "portal:" + UUID.randomUUID() + "@" + config.getPortalId();

        this.grpcServer = NettyServerBuilder.forAddress(new InetSocketAddress(config.getHost(), config.getApiPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(new PortalApiImpl(this))
            .build();

        this.snapshots = new SnapshotSlotsProvider();
    }

    @SuppressWarnings("UnstableApiUsage")
    public void start() {
        LOG.info("Starting portal with ID: '{}' at {}://{}:{}/{} with fs at {}:{}", config.getPortalId(),
            UriScheme.LzyServant.scheme(), config.getHost(), config.getApiPort(), config.getFsPort(),
            config.getHost(), config.getFsPort());

        try {
            grpcServer.start();

            allocatorAgent = new AllocatorAgent(config.getToken(), config.getVmId(),
                config.getAllocatorAddress(), config.getAllocatorHeartbeatPeriod());

            var fsUri = new URI(LzyFs.scheme(), null, config.getHost(), config.getFsPort(), null, null, null);
            var cm = HostAndPort.fromString(config.getChannelManagerAddress());
            var channelManagerUri = new URI("http", null, cm.getHost(), cm.getPort(), null, null, null);

            fsServer = new LzyFsServer(config.getPortalId(), config.getFsRoot(), fsUri, channelManagerUri,
                config.getToken());

        } catch (IOException | URISyntaxException | AllocatorAgent.RegisterException e) {
            LOG.error(e);
            this.shutdown();
            throw new RuntimeException(e);
        }

        var prev = fsServer.setSlotApiInterceptor(new FsApiImpl(this));
        assert prev == null;

        final SlotsManager slotsManager = fsServer.getSlotsManager();
        stdoutSlot = new StdoutSlot(
            stdoutSlotName,
            portalTaskId,
            config.getStdoutChannelId(),
            slotsManager.resolveSlotUri(portalTaskId, stdoutSlotName)
        );
        slotsManager.registerSlot(stdoutSlot);

        stderrSlot = new StdoutSlot(
            stderrSlotName,
            portalTaskId, config.getStderrChannelId(),
            slotsManager.resolveSlotUri(portalTaskId, stderrSlotName)
        );
        slotsManager.registerSlot(stderrSlot);
    }

    public void shutdown() {
        LOG.info("Stopping portal");
        grpcServer.shutdown();
        allocatorAgent.shutdown();
        fsServer.stop();
    }

    public void shutdownNow() {
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

    public SlotsManager getSlotManager() {
        return fsServer.getSlotsManager();
    }

    public String getPortalTaskId() {
        return portalTaskId;
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

    StdoutSlot[] getOutErrSlots() {
        return new StdoutSlot[] {stdoutSlot, stderrSlot};
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
