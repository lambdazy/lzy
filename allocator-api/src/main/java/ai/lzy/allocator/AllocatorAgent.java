package ai.lzy.allocator;

import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.AllocatorPrivateGrpc;
import ai.lzy.v1.VmAllocatorPrivateApi;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Base64;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

public class AllocatorAgent extends TimerTask {
    private static final Logger LOG = LogManager.getLogger(AllocatorAgent.class);

    public static final String VM_ID_KEY = "LZY_ALLOCATOR_VM_ID";
    public static final String VM_ALLOCATOR_ADDRESS = "LZY_ALLOCATOR_ADDRESS";
    public static final String VM_HEARTBEAT_PERIOD = "LZY_ALLOCATOR_HEARTBEAT_PERIOD";
    public static final String VM_ALLOCATOR_OTT = "LZY_ALLOCATOR_OTT";
    public static final String VM_IP_ADDRESS = "LZY_VM_IP_ADDRESS";
    public static final String VM_NODE_IP_ADDRESS = "LZY_VM_NODE_IP_ADDRESS";

    private final String vmId;
    private final AllocatorPrivateGrpc.AllocatorPrivateBlockingStub stub;
    private final Duration heartbeatPeriod;
    private final Timer timer;
    private final ManagedChannel channel;
    private final String vmIpAddress;

    private final ClientHeaderInterceptor<String> authInterceptor;

    public AllocatorAgent(@Nullable String ott, @Nullable String vmId, @Nullable String allocatorAddress,
                          @Nullable Duration heartbeatPeriod, String vmIpAddress)
    {
        this.vmId = vmId == null ? System.getenv(VM_ID_KEY) : vmId;
        final var allocAddress = allocatorAddress == null
            ? System.getenv(VM_ALLOCATOR_ADDRESS) : allocatorAddress;
        this.heartbeatPeriod = heartbeatPeriod == null ? Duration.parse(System.getenv(VM_HEARTBEAT_PERIOD))
            : heartbeatPeriod;
        this.vmIpAddress = vmIpAddress;

        channel = ChannelBuilder.forAddress(allocAddress)
            .usePlaintext()
            .enableRetry(AllocatorPrivateGrpc.SERVICE_NAME)
            .build();
        stub = AllocatorPrivateGrpc.newBlockingStub(channel);

        ott = ott != null ? ott : System.getenv(VM_ALLOCATOR_OTT);

        Objects.requireNonNull(this.vmId);
        Objects.requireNonNull(this.vmIpAddress);
        Objects.requireNonNull(this.heartbeatPeriod);
        Objects.requireNonNull(ott);

        var auth = Base64.getEncoder().encodeToString((this.vmId + '/' + ott).getBytes());
        authInterceptor = ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, () -> auth);

        timer = new Timer("allocator-agent-timer-" + this.vmId);
    }

    public void start() throws RegisterException {
        LOG.info("Register vm with id '{}' in allocator", vmId);

        try {
            //noinspection ResultOfMethodCallIgnored
            stub.withInterceptors(authInterceptor).register(
                VmAllocatorPrivateApi.RegisterRequest.newBuilder()
                    .setVmId(vmId)
                    .putMetadata(VM_IP_ADDRESS, vmIpAddress)
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Cannot register allocator", e);
            throw new RegisterException(e);
        }

        timer.scheduleAtFixedRate(this, heartbeatPeriod.toMillis(), heartbeatPeriod.toMillis());
    }

    @Override
    public void run() {
        try {
            //noinspection ResultOfMethodCallIgnored
            stub.heartbeat(VmAllocatorPrivateApi.HeartbeatRequest.newBuilder()
                .setVmId(vmId)
                .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Cannot send heartbeat to allocator", e);
        }
    }

    public void shutdown() {
        timer.cancel();
        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Error while stopping allocator agent", e);
        }
    }

    public static class RegisterException extends Exception {
        public RegisterException(Throwable e) {
            super(e);
        }
    }
}
