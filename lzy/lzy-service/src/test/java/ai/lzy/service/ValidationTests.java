package ai.lzy.service;

import ai.lzy.v1.common.LMST;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.function.ThrowingRunnable;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public interface ValidationTests<T extends Message> {
    ThrowingRunnable action(T request);

    static String startWorkflow(LzyWorkflowServiceBlockingStub authLzyGrpcClient, String workflowName) {
        return withIdempotencyKey(authLzyGrpcClient, "start_wf").startWorkflow(
            LWFS.StartWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setSnapshotStorage(LMST.StorageConfig.getDefaultInstance())
                .build()).getExecutionId();
    }

    static void finishWorkflow(LzyWorkflowServiceBlockingStub authLzyGrpcClient,
                               String workflowName, String executionId)
    {
        // noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(authLzyGrpcClient, "finish_wf").finishWorkflow(
            LWFS.FinishWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setReason("no-matter")
                .build());
    }

    default void doAssert(T request) {
        var sre = assertThrows(StatusRuntimeException.class, action(request));
        assertSame(Status.INVALID_ARGUMENT.getCode(), sre.getStatus().getCode());
    }
}
