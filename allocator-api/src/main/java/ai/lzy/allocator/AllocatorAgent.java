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

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class AllocatorAgent extends TimerTask {
    private static final Logger LOG = LogManager.getLogger(AllocatorAgent.class);

    public static final String VM_ID_KEY = "LZY_ALLOCATOR_VM_ID";
    public static final String VM_ALLOCATOR_ADDRESS = "LZY_ALLOCATOR_ADDRESS";
    public static final String VM_HEARTBEAT_PERIOD = "LZY_ALLOCATOR_HEARTBEAT_PERIOD";
    public static final String VM_IP_ADDRESS = "LZY_VM_IP_ADDRESS";

    private final String vmId;
    private final AllocatorPrivateGrpc.AllocatorPrivateBlockingStub stub;
    private final Timer timer;
    private final ManagedChannel channel;

    public AllocatorAgent(String iamToken, @Nullable String vmId, @Nullable String allocatorAddress,
                          @Nullable Duration heartbeatPeriod) throws RegisterException
     {
        this.vmId = vmId == null ? System.getenv(VM_ID_KEY) : vmId;
        final var allocAddress = allocatorAddress == null
            ? System.getenv(VM_ALLOCATOR_ADDRESS) : allocatorAddress;
        final var period = heartbeatPeriod == null
            ? Duration.parse(System.getenv(VM_HEARTBEAT_PERIOD)) : heartbeatPeriod;

        channel = ChannelBuilder.forAddress(allocAddress)
            .usePlaintext()
            .enableRetry(AllocatorPrivateGrpc.SERVICE_NAME)
            .build();
        stub = AllocatorPrivateGrpc.newBlockingStub(channel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, () -> iamToken));

        try {
            stub.register(VmAllocatorPrivateApi.RegisterRequest.newBuilder()
                .setVmId(vmId)
                .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Cannot register allocator", e);
            throw new RegisterException(e);
        }
        timer = new Timer("allocator-agent-timer-" + vmId);
        timer.scheduleAtFixedRate(this, period.toMillis(), period.toMillis());
    }

    @Override
    public void run() {
        try {
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
