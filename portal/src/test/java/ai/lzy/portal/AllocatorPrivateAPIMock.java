package ai.lzy.portal;

import ai.lzy.v1.AllocatorPrivateGrpc;
import ai.lzy.v1.VmAllocatorPrivateApi;
import io.grpc.stub.StreamObserver;

class AllocatorPrivateAPIMock extends AllocatorPrivateGrpc.AllocatorPrivateImplBase {
    @Override
    public void register(VmAllocatorPrivateApi.RegisterRequest request,
                         StreamObserver<VmAllocatorPrivateApi.RegisterResponse> responseObserver)
    {
        responseObserver.onNext(VmAllocatorPrivateApi.RegisterResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(VmAllocatorPrivateApi.HeartbeatRequest request,
                          StreamObserver<VmAllocatorPrivateApi.HeartbeatResponse> responseObserver)
    {
        responseObserver.onNext(VmAllocatorPrivateApi.HeartbeatResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
