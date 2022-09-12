package ai.lzy.portal;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.SlotsManager;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.portal.config.PortalConfig;
import ai.lzy.portal.slots.SnapshotSlotsProvider;
import ai.lzy.portal.slots.StdoutSlot;
import ai.lzy.util.grpc.ChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static ai.lzy.model.UriScheme.LzyServant;

public class Portal {
    private static final Logger LOG = LogManager.getLogger(Portal.class);

    private static final String stdoutSlotName = "/portal:stdout";
    private static final String stderrSlotName = "/portal:stderr";

    private final String stdoutChannelId;
    private final String stderrChannelId;

    private final int port;
    private final String host;

    // services
    private final Server grpcServer;
    private final LzyFsServer fsServer;
    private final AllocatorAgent allocatorAgent;

    // slots
    private StdoutSlot stdoutSlot;
    private StdoutSlot stderrSlot;
    private final SnapshotSlotsProvider snapshots;

    private final String portalTaskId;

    public Portal(PortalConfig config, AllocatorAgent agent, LzyFsServer fs) {
        this.stdoutChannelId = config.getStdoutChannelId();
        this.stderrChannelId = config.getStderrChannelId();

        this.port = config.getPortalApiPort();
        this.host = config.getHost();

        this.grpcServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(config.getHost(), config.getPortalApiPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(new PortalApiImpl(this))
            .build();
        this.allocatorAgent = agent;
        this.fsServer = fs;

        var prev = fsServer.setSlotApiInterceptor(new FsApiImpl(this));
        assert prev == null;

        this.snapshots = new SnapshotSlotsProvider();
        this.portalTaskId = "portal:" + config.getPortalId();
    }

    public void start() {
        LOG.info("Starting portal with portal task ID: '{}' at {}://{}:{}/{}",
            portalTaskId, LzyServant.scheme(), host, port, fsServer.getMountPoint());

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

        final SlotsManager slotsManager = fsServer.getSlotsManager();
        stdoutSlot = new StdoutSlot(stdoutSlotName, portalTaskId, stdoutChannelId,
            slotsManager.resolveSlotUri(portalTaskId, stdoutSlotName));
        slotsManager.registerSlot(stdoutSlot);

        stderrSlot = new StdoutSlot(stderrSlotName, portalTaskId, stderrChannelId,
            slotsManager.resolveSlotUri(portalTaskId, stderrSlotName));
        slotsManager.registerSlot(stderrSlot);

        LOG.info("Portal successfully started at '{}:{}'", host, port);
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
