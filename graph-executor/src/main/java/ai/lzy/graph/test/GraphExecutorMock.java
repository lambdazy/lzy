package ai.lzy.graph.test;

import ai.lzy.v1.graph.GraphExecutor;
import ai.lzy.v1.graph.GraphExecutorApi.GraphExecuteRequest;
import ai.lzy.v1.graph.GraphExecutorApi.GraphExecuteResponse;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import io.grpc.stub.StreamObserver;

import java.util.UUID;

public class GraphExecutorMock extends GraphExecutorGrpc.GraphExecutorImplBase {
    @Override
    public void execute(GraphExecuteRequest request, StreamObserver<GraphExecuteResponse> responseObserver) {
        var graphId = UUID.randomUUID().toString();
        responseObserver.onNext(GraphExecuteResponse.newBuilder()
            .setStatus(GraphExecutor.GraphExecutionStatus.newBuilder()
                .setCompleted(GraphExecutor.GraphExecutionStatus.Completed.getDefaultInstance())
                .setWorkflowId(request.getWorkflowId())
                .setGraphId(graphId))
            .build());
        responseObserver.onCompleted();
    }
}
