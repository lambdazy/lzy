package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.impl.kuber.TunnelAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.SessionDao;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import ai.lzy.allocator.disk.DiskStorage;
import ai.lzy.allocator.services.AllocatorApi;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.longrunning.LongRunning;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import lombok.Setter;

@Singleton
@Requires(beans = MetricReporter.class, env = "test")
@Setter
public class AllocatorMock extends AllocatorApi {

    private Runnable onCreateSession = () -> {};
    private Runnable onDeleteSession = () -> {};
    private Runnable onAllocate = () -> {};
    private Runnable onFree = () -> {};

    public AllocatorMock(VmDao dao, OperationDao operations, SessionDao sessions, DiskStorage diskStorage,
                         VmAllocator allocator, TunnelAllocator tunnelAllocator, ServiceConfig config,
                         AllocatorDataSource storage, @Named("AllocatorIamGrpcChannel") ManagedChannel iamChannel,
                         @Named("AllocatorIamToken") RenewableJwt iamToken)
    {
        super(dao, operations, sessions, diskStorage, allocator, tunnelAllocator, config, storage, iamChannel,
            iamToken);
    }

    @Override
    public void createSession(VmAllocatorApi.CreateSessionRequest request,
                              StreamObserver<VmAllocatorApi.CreateSessionResponse> responseObserver)
    {
        onCreateSession.run();
        super.createSession(request, responseObserver);
    }

    @Override
    public void deleteSession(VmAllocatorApi.DeleteSessionRequest request,
                              StreamObserver<VmAllocatorApi.DeleteSessionResponse> responseObserver)
    {
        onDeleteSession.run();
        super.deleteSession(request, responseObserver);
    }

    @Override
    public void allocate(VmAllocatorApi.AllocateRequest request,
                         StreamObserver<LongRunning.Operation> responseObserver)
    {
        onAllocate.run();
        super.allocate(request, responseObserver);
    }

    @Override
    public void free(VmAllocatorApi.FreeRequest request,
                     StreamObserver<VmAllocatorApi.FreeResponse> responseObserver)
    {
        onFree.run();
        super.free(request, responseObserver);
    }
}
