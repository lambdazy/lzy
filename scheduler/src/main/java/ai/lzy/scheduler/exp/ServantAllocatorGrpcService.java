package ai.lzy.scheduler.exp;

import ai.lzy.model.GrpcConverter;
import ai.lzy.v1.exp.ServantAllocatorApi.AllocateServantRequest;
import ai.lzy.v1.exp.ServantAllocatorApi.AllocateServantResponse;
import ai.lzy.v1.exp.ServantAllocatorGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

@Singleton
public class ServantAllocatorGrpcService extends ServantAllocatorGrpc.ServantAllocatorImplBase {

    private final ServantAllocatorExp allocator;

    @Inject
    public ServantAllocatorGrpcService(ServantAllocatorExp allocator) {
        this.allocator = allocator;
    }

    @Override
    public void allocateServant(AllocateServantRequest request, StreamObserver<AllocateServantResponse> response) {
        try {
            var allocation = allocator.allocate(request.getUserId(), request.getExecutionId(),
                GrpcConverter.from(request.getProvisioning()), Duration.ofSeconds(request.getTimeoutSec()));

            response.onNext(
                AllocateServantResponse.newBuilder()
                    .setServantId(allocation.servantId())
                    .setServantAddress(allocation.apiEndpoint().toString())
                    .setServantFsAddress(allocation.fsApiEndpoint().toString())
                    .putAllMeta(allocation.meta())
                    .build());
            response.onCompleted();
        } catch (TimeoutException e) {
            response.onError(Status.DEADLINE_EXCEEDED.withDescription(e.getMessage()).asException());
        } catch (ServantAllocatorExp.ServantAllocationException e) {
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        }
    }
}
