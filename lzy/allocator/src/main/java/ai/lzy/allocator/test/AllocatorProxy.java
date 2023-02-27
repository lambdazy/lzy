package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.AllocationContext;
import ai.lzy.allocator.alloc.dao.SessionDao;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.disk.dao.DiskDao;
import ai.lzy.allocator.services.AllocatorService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.longrunning.LongRunning;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import lombok.Setter;

@Singleton
@Requires(env = "test-mock")
@Setter
public class AllocatorProxy extends AllocatorService {

    private volatile Runnable onCreateSession = () -> {};
    private volatile Runnable onDeleteSession = () -> {};
    private volatile Runnable onAllocate = () -> {};
    private volatile Runnable onFree = () -> {};

    public AllocatorProxy(VmDao vmDao, @Named("AllocatorOperationDao") OperationDao operationsDao,
                          SessionDao sessionsDao, DiskDao diskDao, AllocationContext allocationContext,
                          ServiceConfig config, ServiceConfig.CacheLimits cacheLimits)
    {
        super(vmDao, operationsDao, sessionsDao, diskDao, allocationContext, config, cacheLimits);
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
                              StreamObserver<LongRunning.Operation> responseObserver)
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
