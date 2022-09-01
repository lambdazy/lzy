package ai.lzy.allocator.mock;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.v1.OperationService;
import ai.lzy.v1.OperationServiceApiGrpc;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;

public class OperationServiceMock extends OperationServiceApiGrpc.OperationServiceApiImplBase {
    static final String operationId = "mock-operation";

    @Override
    public void get(OperationService.GetOperationRequest request,
                    StreamObserver<OperationService.Operation> responseObserver) {
        responseObserver.onNext(OperationService.Operation.newBuilder()
            .setId(request.getOperationId())
            .setDone(true)
            .setResponse(Any.pack(VmAllocatorApi.AllocateResponse.newBuilder()
                .putMetadata(AllocatorAgent.VM_IP_ADDRESS, "localhost:0").build()))
            .build());
        responseObserver.onCompleted();
    }
}
