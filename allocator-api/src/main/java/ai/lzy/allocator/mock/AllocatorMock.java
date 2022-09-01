package ai.lzy.allocator.mock;

import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.OperationService;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;

public class AllocatorMock extends AllocatorGrpc.AllocatorImplBase {
    private static final String sessionId = "mock-session-id";
    private static final String vmId = "mock-vm";

    @Override
    public void createSession(VmAllocatorApi.CreateSessionRequest request,
                              StreamObserver<VmAllocatorApi.CreateSessionResponse> responseObserver) {
        responseObserver.onNext(VmAllocatorApi.CreateSessionResponse.newBuilder()
            .setSessionId(sessionId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteSession(VmAllocatorApi.DeleteSessionRequest request,
                              StreamObserver<VmAllocatorApi.DeleteSessionResponse> responseObserver) {
        responseObserver.onNext(VmAllocatorApi.DeleteSessionResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void allocate(VmAllocatorApi.AllocateRequest request,
                         StreamObserver<OperationService.Operation> responseObserver) {
        responseObserver.onNext(OperationService.Operation.newBuilder()
            .setId(OperationServiceMock.operationId)
            .setMetadata(Any.pack(VmAllocatorApi.AllocateMetadata.newBuilder().setVmId(vmId).build()))
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void free(VmAllocatorApi.FreeRequest request, StreamObserver<VmAllocatorApi.FreeResponse> responseObserver) {
        responseObserver.onNext(VmAllocatorApi.FreeResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
