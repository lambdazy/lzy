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
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.lzy.model.UriScheme.LzyFs;

@Singleton
public class Portal implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(Portal.class);

    private final String portalTaskId;

    private final Server grpcServer;
    private final LzyFsServer fsServer;
    private final AllocatorAgent allocatorAgent;

    // stdout/stderr (guarded by this)
    private final StdoutSlot stdoutSlot;
    private final StdoutSlot stderrSlot;
    private final SnapshotSlotsProvider snapshots;
    // common
    private final AtomicBoolean active = new AtomicBoolean(true);

    @SuppressWarnings("UnstableApiUsage")
    public Portal(PortalConfig config) {
        LOG.info("Starting portal at {}://{}:{}/{} with fs at {}:{}", UriScheme.LzyServant.scheme(),
            config.getHost(), config.getApiPort(), config.getFsPort(), config.getHost(), config.getFsPort());

        portalTaskId = "portal:" + UUID.randomUUID() + "@" + config.getServantId();

        grpcServer = NettyServerBuilder.forAddress(new InetSocketAddress(config.getHost(), config.getApiPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(new PortalApiImpl(this))
            .build();

        try {
            grpcServer.start();
            allocatorAgent = new AllocatorAgent(config.getToken(), config.getVmId(),
                config.getAllocatorAddress(), config.getAllocatorHeartbeatPeriod());

            final var fsUri = new URI(LzyFs.scheme(), null, config.getHost(), config.getFsPort(), null, null, null);
            final var cm = HostAndPort.fromString(config.getChannelManagerAddress());
            final var channelManagerUri = new URI("http", null, cm.getHost(), cm.getPort(), null, null, null);

            fsServer = new LzyFsServer(config.getServantId(), config.getFsRoot(),
                fsUri, channelManagerUri, config.getToken());

        } catch (IOException | AllocatorAgent.RegisterException | URISyntaxException e) {
            LOG.error(e);
            this.close();
            throw new RuntimeException(e);
        }

        var prev = fsServer.setSlotApiInterceptor(new FsApiImpl(this));
        assert prev == null;

        final String stdoutSlotName = "/portal:stdout";
        final String stderrSlotName = "/portal:stderr";
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

        snapshots = new SnapshotSlotsProvider();
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

    public boolean isActive() {
        return active.get();
    }

    public void awaitTermination() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    @Override
    public void close() {
        if (active.compareAndSet(true, false)) {
            LOG.error("Stopping portal");
            grpcServer.shutdown();
            allocatorAgent.shutdown();
            fsServer.stop();
        }
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
