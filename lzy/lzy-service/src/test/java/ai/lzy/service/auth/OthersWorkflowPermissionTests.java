package ai.lzy.service.auth;

import ai.lzy.service.WithoutWbAndSchedulerLzyContextTests;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import static ai.lzy.service.IamUtils.authorize;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class OthersWorkflowPermissionTests extends WithoutWbAndSchedulerLzyContextTests {
    private static final String workflowName = "test-workflow";
    private static final String graphId = "test-graph-id";
    private static final String reason = "no-matter";
    private String executionId;

    private LzyWorkflowServiceBlockingStub authGrpcClient1;
    private LzyWorkflowServiceBlockingStub authGrpcClient2;

    @Before
    public void before() throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, InterruptedException {
        authGrpcClient1 = authorize(lzyClient, "test-user-1", iamClient);
        authGrpcClient2 = authorize(lzyClient, "test-user-2", iamClient);
        executionId = startWorkflow(authGrpcClient1, workflowName);
    }

    @After
    public void after() {
        finishWorkflow(authGrpcClient1, workflowName, executionId);
    }

    @Test
    public void finishOthersWorkflowForbidden() {
        var request = LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setReason(reason)
            .build();

        //noinspection ResultOfMethodCallIgnored
        var sre = assertThrows(StatusRuntimeException.class, () ->
            withIdempotencyKey(authGrpcClient2, "finish_wf_" + workflowName).finishWorkflow(request));
        assertSame(Status.INVALID_ARGUMENT.getCode(), sre.getStatus().getCode());
    }

    @Test
    public void abortOthersWorkflowForbidden() {
        var request = LWFS.AbortWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setReason(reason)
            .build();

        //noinspection ResultOfMethodCallIgnored
        var sre = assertThrows(StatusRuntimeException.class, () ->
            withIdempotencyKey(authGrpcClient2, "abort_wf_" + workflowName).abortWorkflow(request));
        assertSame(Status.INVALID_ARGUMENT.getCode(), sre.getStatus().getCode());
    }

    @Test
    public void executeGraphInOthersWorkflowForbidden() {
        var request = LWFS.ExecuteGraphRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraph(LWF.Graph.getDefaultInstance())
            .build();

        //noinspection ResultOfMethodCallIgnored
        var sre = assertThrows(StatusRuntimeException.class, () ->
            withIdempotencyKey(authGrpcClient2, "execute_graph_" + workflowName).executeGraph(request));
        assertSame(Status.INVALID_ARGUMENT.getCode(), sre.getStatus().getCode());
    }

    @Test
    public void getStatusOfOthersGraphForbidden() {
        var request = LWFS.GraphStatusRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraphId(graphId)
            .build();

        //noinspection ResultOfMethodCallIgnored
        var sre = assertThrows(StatusRuntimeException.class, () -> authGrpcClient2.graphStatus(request));
        assertSame(Status.INVALID_ARGUMENT.getCode(), sre.getStatus().getCode());
    }

    @Test
    public void stopOthersGraphForbidden() {
        var request = LWFS.StopGraphRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraphId("test-graph-id")
            .build();

        //noinspection ResultOfMethodCallIgnored
        var sre = assertThrows(StatusRuntimeException.class, () ->
            withIdempotencyKey(authGrpcClient2, "stop_graph_" + workflowName).stopGraph(request));
        assertSame(Status.INVALID_ARGUMENT.getCode(), sre.getStatus().getCode());
    }

    @Test
    public void readStdSlotsOtherUsersForbidden() {
        var request = LWFS.ReadStdSlotsRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .build();

        var sre = assertThrows(StatusRuntimeException.class, () -> authGrpcClient2.readStdSlots(request).next());
        assertSame(Status.INVALID_ARGUMENT.getCode(), sre.getStatus().getCode());
    }

    @Test
    public void getAvailablePoolsOfOthersWorkflowForbidden() {
        var request = LWFS.GetAvailablePoolsRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .build();

        //noinspection ResultOfMethodCallIgnored
        var sre = assertThrows(StatusRuntimeException.class, () -> authGrpcClient2.getAvailablePools(request));
        assertSame(Status.INVALID_ARGUMENT.getCode(), sre.getStatus().getCode());
    }

    @Test
    public void privateAbortIsInternalOnly() {
        var request = LWFS.AbortWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setReason(reason)
            .build();

        //noinspection ResultOfMethodCallIgnored
        var sre = assertThrows(StatusRuntimeException.class, () ->
            lzyServiceTestContext.privateGrpcClient().abortWorkflow(request)
        );
        assertSame(Status.UNAUTHENTICATED.getCode(), sre.getStatus().getCode());
    }
}
