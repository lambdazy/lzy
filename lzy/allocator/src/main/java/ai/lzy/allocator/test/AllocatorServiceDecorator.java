package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.AllocationContext;
import ai.lzy.allocator.alloc.dao.SessionDao;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.disk.dao.DiskDao;
import ai.lzy.allocator.services.AllocatorService;
import ai.lzy.common.IdGenerator;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.util.function.Consumer;

@Singleton
@Requires(env = "test-mock")
public class AllocatorServiceDecorator extends AllocatorService {
    private volatile Consumer<String> onCreateSession = sessionId -> {};
    private volatile Consumer<String> onDeleteSession = sessionId -> {};
    private volatile Consumer<String> onAllocate = vmId -> {};
    private volatile Consumer<String> onFree = vmId -> {};

    public AllocatorServiceDecorator(VmDao vmDao, @Named("AllocatorOperationDao") OperationDao operationsDao,
                                     SessionDao sessionsDao, DiskDao diskDao, AllocationContext allocationContext,
                                     ServiceConfig config, ServiceConfig.CacheLimits cacheLimits,
                                     ServiceConfig.MountConfig mountConfig,
                                     @Named("AllocatorIdGenerator") IdGenerator idGenerator)
    {
        super(vmDao, operationsDao, sessionsDao, diskDao, allocationContext, config, cacheLimits, mountConfig,
            idGenerator);
    }

    @Override
    public void createSession(VmAllocatorApi.CreateSessionRequest request,
                              StreamObserver<LongRunning.Operation> responseObserver)
    {
        super.createSession(request, new StreamObserver<>() {
            @Override
            public void onNext(LongRunning.Operation operation) {
                try {
                    onCreateSession.accept(
                        operation.getResponse().unpack(VmAllocatorApi.CreateSessionResponse.class).getSessionId()
                    );
                } catch (InvalidProtocolBufferException e) {
                    // intentionally blank
                }
                responseObserver.onNext(operation);
            }

            @Override
            public void onError(Throwable throwable) {
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        });
    }

    @Override
    public void deleteSession(VmAllocatorApi.DeleteSessionRequest request,
                              StreamObserver<LongRunning.Operation> responseObserver)
    {
        onDeleteSession.accept(request.getSessionId());
        super.deleteSession(request, responseObserver);
    }

    @Override
    public void allocate(VmAllocatorApi.AllocateRequest request,
                         StreamObserver<LongRunning.Operation> responseObserver)
    {
        super.allocate(request, new StreamObserver<>() {
            @Override
            public void onNext(LongRunning.Operation operation) {
                try {
                    onAllocate.accept(
                        operation.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId()
                    );
                } catch (InvalidProtocolBufferException e) {
                    // intentionally blank
                }
                responseObserver.onNext(operation);
            }

            @Override
            public void onError(Throwable throwable) {
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        });
    }

    @Override
    public void free(VmAllocatorApi.FreeRequest request, StreamObserver<VmAllocatorApi.FreeResponse> responseObserver) {
        onFree.accept(request.getVmId());
        super.free(request, responseObserver);
    }

    public AllocatorServiceDecorator onCreateSession(Consumer<String> onCreateSession) {
        this.onCreateSession = onCreateSession;
        return this;
    }

    public AllocatorServiceDecorator onDeleteSession(Consumer<String> onDeleteSession) {
        this.onDeleteSession = onDeleteSession;
        return this;
    }

    public AllocatorServiceDecorator onAllocate(Consumer<String> onAllocate) {
        this.onAllocate = onAllocate;
        return this;
    }

    public AllocatorServiceDecorator onFree(Consumer<String> onFree) {
        this.onFree = onFree;
        return this;
    }
}
