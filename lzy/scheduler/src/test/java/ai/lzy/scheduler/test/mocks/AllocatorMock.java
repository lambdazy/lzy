package ai.lzy.scheduler.test.mocks;

import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.model.operation.Operation;
import ai.lzy.scheduler.allocator.WorkersAllocator;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.AllocateMetadata;
import ai.lzy.v1.VmAllocatorApi.AllocateResponse.VmEndpoint;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.micronaut.context.annotation.Primary;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Consumer;
import javax.inject.Singleton;

@Singleton
@Primary // for tests only
public class AllocatorMock implements WorkersAllocator {
    public static OnAllocatedRequest onAllocate = (a, b, c) -> "localhost:9090";
    public static Consumer<String> onDestroy = (a) -> {};

    private final LocalOperationService opService;
    private final Server server;

    public AllocatorMock(ServiceConfig config) {
        opService = new LocalOperationService("name");
        server = GrpcUtils.newGrpcServer(HostAndPort.fromString(config.getAllocatorAddress()), null)
            .addService(opService)
            .build();

        try {
            server.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AllocateResult allocate(String userId, String workflowName, String sessionId,
                                          Operation.Requirements requirements)
    {
        var address = onAllocate.call(workflowName, userId, sessionId);

        var addr = HostAndPort.fromString(address);

        var port = addr.getPort();
        var host = addr.getHost();

        var vmId = UUID.randomUUID().toString();
        final String pk;
        try {
            pk = RsaUtils.generateRsaKeys().publicKey();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        var resp = VmAllocatorApi.AllocateResponse.newBuilder()
            .setVmId(vmId)
            .setPoolId("s")
            .setSessionId(sessionId)
            .putMetadata("PUBLIC_KEY", pk)
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

        return new AllocateResult(op.toProto(), port, port);
    }

    @Override
    public String createSession(String userId, String workflowName, String idempotencyKey) {
        return "session-id";
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
