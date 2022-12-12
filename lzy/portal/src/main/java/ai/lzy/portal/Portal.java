package ai.lzy.portal;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.fs.SlotsManager;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.model.Constants;
import ai.lzy.model.slot.Slot;
import ai.lzy.portal.config.PortalConfig;
import ai.lzy.portal.slots.SnapshotProvider;
import ai.lzy.portal.slots.StdoutSlot;
import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.channel.deprecated.LzyChannelManagerGrpc;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import static ai.lzy.model.UriScheme.LzyFs;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;
import static ai.lzy.v1.channel.deprecated.LzyChannelManagerGrpc.newBlockingStub;

public class Portal {
    private static final Logger LOG = LogManager.getLogger(Portal.class);

    public static final String APP = "LzyPortal";

    public static final String PORTAL_SLOT_PREFIX = "/portal_slot";
    public static final String PORTAL_OUT_SLOT_NAME = "/portal_slot:stdout";
    public static final String PORTAL_ERR_SLOT_NAME = "/portal_slot:stderr";

    private final String stdoutChannelId;
    private final String stderrChannelId;

    private final String host;
    private final int portalPort;
    private final int slotsPort;

    private final ManagedChannel iamChannel;
    private final ManagedChannel channelsManagerChannel;

    // services
    private final Server portalServer;
    private final Server slotsServer;
    private final PortalSlotsService portalSlotsService;
    private final AllocatorAgent allocatorAgent;

    private final RenewableJwt slotsJwt;
    private final Supplier<String> tokenFactory;
    private SlotsManager slotsManager;

    // slots
    private StdoutSlot stdoutSlot;
    private StdoutSlot stderrSlot;
    private final SnapshotProvider snapshots;

    private final String portalId;

    private CountDownLatch started;
    private final AtomicBoolean finished = new AtomicBoolean(false);

    public Portal(PortalConfig config, AllocatorAgent agent, @Nullable String testOnlyToken)
        throws IOException, NoSuchAlgorithmException, InvalidKeySpecException
    {
        stdoutChannelId = config.getStdoutChannelId();
        stderrChannelId = config.getStderrChannelId();

        this.host = config.getHost();
        portalPort = config.getPortalApiPort();
        slotsPort = config.getSlotsApiPort();

        iamChannel = newGrpcChannel(config.getIamAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
        channelsManagerChannel = newGrpcChannel(config.getChannelManagerAddress(), LzyChannelManagerGrpc.SERVICE_NAME);

        var internalOnly = new AllowInternalUserOnlyInterceptor(APP, iamChannel);

        portalServer = newGrpcServer(host, portalPort,
            new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel)))
            .addService(ServerInterceptors.intercept(new PortalApiImpl(this), internalOnly))
            .build();

        portalSlotsService = new PortalSlotsService(this);

        slotsServer = newGrpcServer(host, slotsPort, GrpcUtils.NO_AUTH)
            .addService(portalSlotsService.getSlotsApi())
            .addService(portalSlotsService.getLongrunningApi())
            .addService(portalSlotsService.getLegacyWrapper())
            .build();

        allocatorAgent = agent;

        if (testOnlyToken == null) {
            var privateKey = CredentialsUtils.readPrivateKey(config.getIamPrivateKey());
            slotsJwt = new RenewableJwt(config.getPortalId(), "INTERNAL", Duration.ofHours(1), privateKey);
            tokenFactory = () -> slotsJwt.get().token();
        } else {
            slotsJwt = null;
            tokenFactory = () -> testOnlyToken;
        }

        snapshots = new SnapshotProvider();
        portalId = config.getPortalId();
    }

    public void start() {
        started = new CountDownLatch(1);

        LOG.info("Starting portal with config: { portalId: '{}', host: '{}', port: '{}', slotsPort: '{}', " +
                "stdoutChannelId: '{}', stderrChannelId: '{}'}",
            portalId, host, portalPort, slotsPort, stdoutChannelId, stderrChannelId);

        try {
            portalServer.start();

            allocatorAgent.start(Map.of(
                Constants.PORTAL_ADDRESS_KEY, HostAndPort.fromParts(host, portalPort).toString(),
                Constants.FS_ADDRESS_KEY, HostAndPort.fromParts(host, slotsPort).toString()
            ));

            slotsServer.start();
        } catch (IOException | AllocatorAgent.RegisterException e) {
            LOG.error(e);
            this.shutdown();
            throw new RuntimeException(e);
        }

        LOG.info("Registering portal stdout/err slots...");

        var stdoutSlotName = PORTAL_SLOT_PREFIX + ":" + Slot.STDOUT_SUFFIX;
        var stderrSlotName = PORTAL_SLOT_PREFIX + ":" + Slot.STDERR_SUFFIX;

        slotsManager = new SlotsManager(
            newBlockingClient(newBlockingStub(channelsManagerChannel), APP, tokenFactory),
            URI.create("%s://%s:%d".formatted(LzyFs.scheme(), host, slotsPort)));

        stdoutSlot = new StdoutSlot(stdoutSlotName, portalId, stdoutChannelId,
            slotsManager.resolveSlotUri(portalId, stdoutSlotName));
        slotsManager.registerSlot(stdoutSlot);

        stderrSlot = new StdoutSlot(stderrSlotName, portalId, stderrChannelId,
            slotsManager.resolveSlotUri(portalId, stderrSlotName));
        slotsManager.registerSlot(stderrSlot);

        LOG.info("Portal successfully started at '{}:{}'", host, portalPort);

        started.countDown();
    }

    public void shutdown() {
        LOG.info("Stopping portal");
        iamChannel.shutdown();
        channelsManagerChannel.shutdown();
        portalServer.shutdown();
        portalSlotsService.shutdown();
        slotsServer.shutdown();
        allocatorAgent.shutdown();
    }

    public void shutdownNow() {
        iamChannel.shutdownNow();
        channelsManagerChannel.shutdownNow();
        portalServer.shutdownNow();
        portalSlotsService.shutdown();
        slotsServer.shutdownNow();
        allocatorAgent.shutdown();
    }

    public void awaitTermination() throws InterruptedException {
        portalServer.awaitTermination();
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean awaitTermination(long count, TimeUnit timeUnit) throws InterruptedException {
        return portalServer.awaitTermination(count, timeUnit);
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

    SnapshotProvider getSnapshots() {
        return snapshots;
    }

    public void finish() {
        LOG.info("Finishing portal with id <{}>", portalId);
        if (!finished.compareAndSet(false, true)) {
            throw new IllegalStateException("Cannot finish already finished portal");
        }

        for (var slot: getSnapshots().getOutputSlots()) {
            try {
                slot.close();
            } catch (Exception e) {
                LOG.error("Cannot close slot <{}>:", slot.name(), e);
            }
        }

        for (var slot: getSnapshots().getInputSlots()) {
            try {
                slot.close();
            } catch (Exception e) {
                LOG.error("Cannot close slot <{}>:", slot.name(), e);
            }
        }

        try {
            getStdoutSlot().finish();
        } catch (Exception e) {
            LOG.error("Cannot finish stdout slot in portal with id <{}>: ", portalId, e);
        }
        try {
            getStderrSlot().finish();
        } catch (Exception e) {
            LOG.error("Cannot finish stderr slot in portal with id <{}>: ", portalId, e);
        }
    }
}
