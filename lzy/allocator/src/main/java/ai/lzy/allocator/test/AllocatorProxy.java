package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.AllocatorMetrics;
import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.dao.SessionDao;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.alloc.impl.kuber.TunnelAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.disk.dao.DiskDao;
import ai.lzy.allocator.services.AllocatorService;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.longrunning.LongRunning;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import lombok.Setter;

@Singleton
@Requires(beans = MetricReporter.class, env = "test-mock")
@Setter
public class AllocatorProxy extends AllocatorService {

    private volatile Runnable onCreateSession = () -> {};
    private volatile Runnable onDeleteSession = () -> {};
    private volatile Runnable onAllocate = () -> {};
    private volatile Runnable onFree = () -> {};

    public AllocatorProxy(VmDao vmDao, @Named("AllocatorOperationDao") OperationDao operationsDao,
                          SessionDao sessionsDao, DiskDao diskDao, VmAllocator allocator,
                          TunnelAllocator tunnelAllocator, ServiceConfig config, AllocatorDataSource storage,
                          AllocatorMetrics metrics, @Named("AllocatorOperationsExecutor") OperationsExecutor executor,
                          @Named("AllocatorSubjectServiceClient") SubjectServiceGrpcClient subjectClient)
    {
        super(vmDao, operationsDao, sessionsDao, diskDao, allocator, tunnelAllocator, config, storage, metrics,
            executor, subjectClient);
    }

    @Override
    public void createSession(VmAllocatorApi.CreateSessionRequest request,
                              StreamObserver<LongRunning.Operation> responseObserver)
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
