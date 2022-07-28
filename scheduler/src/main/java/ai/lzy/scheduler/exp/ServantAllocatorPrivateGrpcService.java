package ai.lzy.scheduler.exp;

import ai.lzy.v1.exp.ServantAllocatorApi;
import ai.lzy.v1.exp.ServantAllocatorApi.RegisterServantRequest;
import ai.lzy.v1.exp.ServantAllocatorApi.RegisterServantResponse;
import ai.lzy.v1.exp.ServantAllocatorApi.ServantStatusRequest;
import ai.lzy.v1.exp.ServantAllocatorApi.ServantStatusResponse;
import ai.lzy.v1.exp.ServantAllocatorPrivateGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class ServantAllocatorPrivateGrpcService extends ServantAllocatorPrivateGrpc.ServantAllocatorPrivateImplBase {

    private final ServantAllocatorExp.Ex allocator;

    @Inject
    public ServantAllocatorPrivateGrpcService(ServantAllocatorExp.Ex allocator) {
        this.allocator = allocator;
    }

    @Override
    public void registerServant(RegisterServantRequest request, StreamObserver<RegisterServantResponse> response) {
        var ott = "ott"; // take it from header
        allocator.register(request.getServantId(), ott, HostAndPort.fromString(request.getServantAddress()),
            HostAndPort.fromString(request.getServantFsAddress()), request.getMetaMap());

        response.onNext(
            RegisterServantResponse.newBuilder()
                .setAuth(ServantAllocatorApi.ServantAuth.newBuilder()
                    .setIamToken("token")
                    .build())
                .build());
        response.onCompleted();
    }

    @Override
    public void servantStatus(ServantStatusRequest request, StreamObserver<ServantStatusResponse> response) {
        var state = switch (request.getStatus().getStatusCase()) {
            case SERVANTSTARTED -> ServantAllocatorExp.Ex.ServantState.Start;
            case STARTTASK -> ServantAllocatorExp.Ex.ServantState.StartTask;
            case EXECUTETASK -> ServantAllocatorExp.Ex.ServantState.ExecuteTask;
            case FINISHTASK -> ServantAllocatorExp.Ex.ServantState.FinishTask;
            case IDLE -> ServantAllocatorExp.Ex.ServantState.Idle;
            case FINISHED -> ServantAllocatorExp.Ex.ServantState.Finish;
            case STATUS_NOT_SET -> throw new AssertionError();
        };
        allocator.report(request.getServantId(), state);

        response.onNext(
            ServantStatusResponse.newBuilder()
                .setAuth(ServantAllocatorApi.ServantAuth.newBuilder()
                    .setIamToken("token")
                    .build())
                .build());
        response.onCompleted();
    }
}
