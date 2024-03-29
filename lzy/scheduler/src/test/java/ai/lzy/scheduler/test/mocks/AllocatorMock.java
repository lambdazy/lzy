package ai.lzy.scheduler.test.mocks;

import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.scheduler.allocator.WorkersAllocator;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.AllocateMetadata;
import ai.lzy.v1.VmAllocatorApi.AllocateResponse.VmEndpoint;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.worker.MetadataConstants;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.micronaut.context.annotation.Primary;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Consumer;

@Singleton
@Primary // for tests only
public class AllocatorMock implements WorkersAllocator {
    public static volatile OnAllocatedRequest onAllocate = (a, b, c) -> "localhost:9090";
    public static volatile Consumer<String> onDestroy = (a) -> {};

    private final LocalOperationService opService;
    private final Server server;

    public AllocatorMock(ServiceConfig config) {
        opService = new LocalOperationService("name");
        server = GrpcUtils.newGrpcServer(HostAndPort.fromString(config.getAllocatorAddress()), GrpcUtils.NO_AUTH)
            .addService(opService)
            .build();

        try {
            server.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public LongRunning.Operation allocate(String userId, String workflowName, String sessionId,
                                          LMO.Requirements requirements)
    {
        var address = onAllocate.call(workflowName, userId, sessionId);

        var addr = HostAndPort.fromString(address);

        var port = addr.getPort();
        var host = addr.getHost();

        var vmId = UUID.randomUUID().toString();

        var resp = VmAllocatorApi.AllocateResponse.newBuilder()
            .setVmId(vmId)
            .setPoolId("s")
            .setSessionId(sessionId)
            .putMetadata(MetadataConstants.API_PORT, String.valueOf(port))
            .addEndpoints(VmEndpoint.newBuilder()
                .setType(VmEndpoint.VmEndpointType.INTERNAL_IP)
                .setValue(host)
                .build())
            .build();

        var op = ai.lzy.longrunning.Operation.create("test", "", null, AllocateMetadata.newBuilder()
            .setVmId(vmId)
            .build());
        opService.registerOperation(op);
        opService.updateResponse(op.id(), resp);

        return op.toProto();
    }

    @Override
    public void free(String vmId) {
        onDestroy.accept(vmId);
    }

    public interface OnAllocatedRequest {
        String call(String workflowName, String userId, String sessionId);
    }

    @PreDestroy
    public void close() {
        server.shutdown();
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        server.shutdownNow();
    }
}
