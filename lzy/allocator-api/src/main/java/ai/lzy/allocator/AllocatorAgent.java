package ai.lzy.allocator;

import ai.lzy.util.auth.credentials.OttHelper;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.v1.AllocatorPrivateGrpc;
import ai.lzy.v1.VmAllocatorPrivateApi;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class AllocatorAgent extends TimerTask {
    private static final Logger LOG = LogManager.getLogger(AllocatorAgent.class);

    public static final String VM_ID_KEY = "LZY_ALLOCATOR_VM_ID";
    public static final String VM_ALLOCATOR_ADDRESS = "LZY_ALLOCATOR_ADDRESS";
    public static final String VM_HEARTBEAT_PERIOD = "LZY_ALLOCATOR_HEARTBEAT_PERIOD";
    public static final String VM_ALLOCATOR_OTT = "LZY_ALLOCATOR_OTT";
    public static final String VM_IP_ADDRESS = "LZY_VM_IP_ADDRESS";
    public static final String VM_NODE_IP_ADDRESS = "LZY_VM_NODE_IP_ADDRESS";
    public static final String VM_GPU_COUNT = "LZY_VM_GPU_COUNT";
    public static final String K8S_POD_NAME = "K8S_POD_NAME";
    public static final String K8S_NAMESPACE = "K8S_NAMESPACE";
    public static final String K8S_CONTAINER_NAME = "K8S_CONTAINER_NAME";

    private final String vmId;
    private final String allocatorAddress;
    private final AllocatorPrivateGrpc.AllocatorPrivateBlockingStub stub;
    private final Duration heartbeatPeriod;
    private final Timer timer;
    private final ManagedChannel channel;

    private final ClientHeaderInterceptor<String> authInterceptor;

    public AllocatorAgent(@Nullable String ott, @Nullable String vmId, @Nullable String allocatorAddress,
                          @Nullable Duration heartbeatPeriod)
    {
        this.vmId = vmId == null ? System.getenv(VM_ID_KEY) : vmId;
        this.allocatorAddress = allocatorAddress == null
            ? System.getenv(VM_ALLOCATOR_ADDRESS)
            : allocatorAddress;
        this.heartbeatPeriod = heartbeatPeriod == null ? Duration.parse(System.getenv(VM_HEARTBEAT_PERIOD))
            : heartbeatPeriod;

        channel = newGrpcChannel(this.allocatorAddress, AllocatorPrivateGrpc.SERVICE_NAME);
        stub = newBlockingClient(AllocatorPrivateGrpc.newBlockingStub(channel), "AllocatorAgent", null);

        ott = ott != null ? ott : System.getenv(VM_ALLOCATOR_OTT);

        Objects.requireNonNull(this.vmId);
        Objects.requireNonNull(this.heartbeatPeriod);
        Objects.requireNonNull(ott);

        authInterceptor = OttHelper.createOttClientInterceptor(this.vmId, ott);

        timer = new Timer("allocator-agent-timer-" + this.vmId);
    }

    public void start(@Nullable Map<String, String> meta) throws RegisterException {
        Map<String, String> m = meta == null ? Map.of() : meta;

        final var deadline = Instant.now().plus(Duration.ofMinutes(3));

        boolean done = false;
        String error = null;

        while (!done && Instant.now().isBefore(deadline)) {
            LOG.info("Register VM '{}' at allocator...", vmId);

            try {
                //noinspection ResultOfMethodCallIgnored
                stub.withInterceptors(authInterceptor)
                    .register(
                        VmAllocatorPrivateApi.RegisterRequest.newBuilder()
                            .setVmId(vmId)
                            .putAllMetadata(m)
                            .build());
                done = true;
                break;
            } catch (StatusRuntimeException e) {
                LOG.error("Cannot register at allocator: [{}] {}",
                    e.getStatus().getCode(), e.getStatus().getDescription());

                switch (e.getStatus().getCode()) {
                    case ALREADY_EXISTS ->
                        done = true;
                    case UNAVAILABLE, ABORTED, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED ->
                        LockSupport.parkNanos(Duration.ofSeconds(1).toNanos());
                    default -> {
                        done = true;
                        error = e.getStatus().toString();
                    }
                }
            }
        }

        if (done) {
            if (error == null) {
                LOG.info("Successfully registered");
            }
        } else {
            error = "Registration timeout";
        }

        if (error != null) {
            LOG.error("Cannot register VM: {}", error);
            throw new RegisterException(error);
        }


        timer.scheduleAtFixedRate(this, heartbeatPeriod.toMillis(), heartbeatPeriod.toMillis());
    }

    public void start() throws RegisterException {
        start(null);
    }

    @Override
    public void run() {
        try {
            //noinspection ResultOfMethodCallIgnored
            stub.heartbeat(VmAllocatorPrivateApi.HeartbeatRequest.newBuilder()
                .setVmId(vmId)
                .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Cannot send heartbeat to allocator {}: [{}] {}",
                allocatorAddress, e.getStatus().getCode(), e.getStatus().getDescription());
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                cancel();
            }
        }
    }

    public void shutdown() {
        timer.cancel();
        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Shutdown interrupted", e);
        }
    }

    public static class RegisterException extends Exception {
        public RegisterException(String message) {
            super(message);
        }
    }
}
