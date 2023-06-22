package ai.lzy.channelmanager.v2;

import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LWFS.AbortWorkflowRequest;
import ai.lzy.v1.workflow.LWFS.AbortWorkflowResponse;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class LzyServiceMock extends LzyWorkflowServiceGrpc.LzyWorkflowServiceImplBase {
    private final Map<String, CompletableFuture<AbortWorkflowRequest>> waiters = new ConcurrentHashMap<>();

    public CompletableFuture<AbortWorkflowRequest> waitForAbort(String executionId) {
        var waiter = new CompletableFuture<AbortWorkflowRequest>();
        waiters.put(executionId, waiter);
        return waiter;
    }

    @Override
    public void abortWorkflow(AbortWorkflowRequest request, StreamObserver<AbortWorkflowResponse> responseObserver) {

        var waiter = waiters.get(request.getExecutionId());
        if (waiter != null) {
            waiter.complete(request);
        }

        responseObserver.onNext(AbortWorkflowResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
