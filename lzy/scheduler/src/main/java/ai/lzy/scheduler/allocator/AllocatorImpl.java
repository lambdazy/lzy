package ai.lzy.scheduler.allocator;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.operation.Operation;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.SchedulerApi;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.configs.WorkerEventProcessorConfig;
import ai.lzy.util.grpc.GrpcChannels;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.AllocateRequest.Workload;
import ai.lzy.v1.VmAllocatorApi.CreateSessionRequest;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import io.grpc.ManagedChannel;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PreDestroy;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;


@Singleton
public class AllocatorImpl implements WorkersAllocator {
    private static final Logger LOG = LogManager.getLogger(AllocatorImpl.class);


    public static final AtomicBoolean randomWorkerPorts = new AtomicBoolean(false);

    private final ServiceConfig config;
    private final WorkerEventProcessorConfig processorConfig;
    private final AllocatorGrpc.AllocatorBlockingStub allocator;
    private final AtomicInteger testWorkerCounter = new AtomicInteger(0);
    private final ManagedChannel iamChannel;
    private final ManagedChannel allocatorChannel;
    private final ManagedChannel opChannel;

    public AllocatorImpl(ServiceConfig config, WorkerEventProcessorConfig processorConfig) {
        this.config = config;
        this.processorConfig = processorConfig;
        IamClientConfiguration authConfig = config.getIam();
        this.iamChannel = newGrpcChannel(authConfig.getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);

        allocatorChannel = newGrpcChannel(config.getAllocatorAddress(), AllocatorGrpc.SERVICE_NAME);
        final var credentials = authConfig.createRenewableToken();
        allocator = newBlockingClient(AllocatorGrpc.newBlockingStub(allocatorChannel), SchedulerApi.APP,
            () -> credentials.get().token());

        opChannel = newGrpcChannel(config.getAllocatorAddress(), LongRunningServiceGrpc.SERVICE_NAME);
    }

    @PreDestroy
    public void shutdown() {
        GrpcChannels.awaitTermination(opChannel, java.time.Duration.ofSeconds(10), getClass());
        GrpcChannels.awaitTermination(allocatorChannel, java.time.Duration.ofSeconds(10), getClass());
        GrpcChannels.awaitTermination(iamChannel, java.time.Duration.ofSeconds(10), getClass());
    }

    @Override
    public String createSession(String userId, String workflowName) {

        var stub = GrpcUtils.withIdempotencyKey(allocator, workflowName);

        final var createSessionOp = stub.createSession(
            CreateSessionRequest.newBuilder()
                .setOwner(userId)
                .setDescription("Worker allocation")
                .setCachePolicy(
                    VmAllocatorApi.CachePolicy.newBuilder()
                        .setIdleTimeout(Durations.fromMinutes(10))
                        .build())
                .build());

        if (!createSessionOp.getDone()) {
            LOG.error("Unexpected create session operation state");
            throw new RuntimeException("Unexpected create session operation state");
        }

        String sessionId;
        try {
            sessionId = createSessionOp.getResponse().unpack(VmAllocatorApi.CreateSessionResponse.class).getSessionId();
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Cannot parse CreateSessionResponse", e);
            throw new RuntimeException(e);
        }

        return sessionId;

    }

    @Override
    public LongRunning.Operation allocate(String userId, String workflowName,
                                          String sessionId, Operation.Requirements requirements)
    {
        final int port;
        final int fsPort;
        final String mountPoint;

        if (randomWorkerPorts.get()) {
            port = FreePortFinder.find(10000, 11000);
            fsPort = FreePortFinder.find(11000, 12000);
            mountPoint = "/tmp/lzy" + testWorkerCounter.incrementAndGet();
        } else {
            port = 9999;
            fsPort = 9988;
            mountPoint = "/tmp/lzy";
        }

        final var ports = Map.of(
            port, port,
            fsPort, fsPort
        );

        final var args = List.of(
            "-p", String.valueOf(port),
            "-q", String.valueOf(fsPort),
            "--channel-manager", config.getChannelManagerAddress(),
            "-i", config.getIam().getAddress(),
            "--lzy-mount", mountPoint
        );

        final var workload = Workload.newBuilder()
            .setName("worker")
            .setImage(config.getWorkerImage())
            .addAllArgs(args)
            .putAllPortBindings(ports)
            .build();

        final var request = VmAllocatorApi.AllocateRequest.newBuilder()
            .setPoolLabel(requirements.poolLabel())
            .setZone(requirements.zone())
            .setSessionId(sessionId)
            .addWorkload(workload)
            .setClusterType(VmAllocatorApi.AllocateRequest.ClusterType.USER)
            .build();

        return allocator.allocate(request);
    }

    @Override
    public void free(String vmId) {
        allocator.free(VmAllocatorApi.FreeRequest.newBuilder()
            .setVmId(vmId)
            .build());
    }
}
