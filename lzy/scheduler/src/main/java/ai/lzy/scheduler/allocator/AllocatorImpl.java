package ai.lzy.scheduler.allocator;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.scheduler.SchedulerApi;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.util.grpc.GrpcChannels;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.AllocateRequest.Workload;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.worker.DefaultPorts;
import io.grpc.ManagedChannel;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;


@Singleton
public class AllocatorImpl implements WorkersAllocator {
    private static final Logger LOG = LogManager.getLogger(AllocatorImpl.class);
    private final ServiceConfig config;
    private final AllocatorGrpc.AllocatorBlockingStub allocator;
    private final ManagedChannel iamChannel;
    private final ManagedChannel allocatorChannel;
    private final ManagedChannel opChannel;

    public AllocatorImpl(ServiceConfig config) {
        this.config = config;
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
    public LongRunning.Operation allocate(String userId, String workflowName,
                                          String sessionId, LMO.Requirements requirements)
    {
        final var args = new java.util.ArrayList<>(List.of(
            "--channel-manager", config.getChannelManagerAddress(),
            "-i", config.getIam().getAddress(),
            "--fs-port", String.valueOf(DefaultPorts.FS_PORT),
            "--api-port", String.valueOf(DefaultPorts.API_PORT),
            "--http-port", String.valueOf(DefaultPorts.HTTP_PORT)
        ));

        args.add("--kafka-bootstrap");
        args.add(String.join(",", config.getKafka().getBootstrapServers()));

        if (config.getKafka().isTlsEnabled()) {
            try (var file = new FileInputStream(config.getKafka().getTlsTruststorePath())) {
                var bytes = file.readAllBytes();

                args.add("--truststore-base64");
                args.add(Base64.getEncoder().encodeToString(bytes));

                args.add("--truststore-password");
                args.add(config.getKafka().getTlsTruststorePassword());
            } catch (IOException e) {
                LOG.error("Cannot serialize kafka CA", e);
                throw new RuntimeException("Cannot serialize kafka CA", e);
            }
        }

        final var workload = Workload.newBuilder()
            .setName("worker")
            .setImage(config.getWorkerImage())
            .putAllPortBindings(Map.of(
                DefaultPorts.FS_PORT, DefaultPorts.FS_PORT,
                DefaultPorts.API_PORT, DefaultPorts.API_PORT,
                DefaultPorts.HTTP_PORT, DefaultPorts.HTTP_PORT))
            .addAllArgs(args)
            .build();

        final var request = VmAllocatorApi.AllocateRequest.newBuilder()
            .setPoolLabel(requirements.getPoolLabel())
            .setZone(requirements.getZone())
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
