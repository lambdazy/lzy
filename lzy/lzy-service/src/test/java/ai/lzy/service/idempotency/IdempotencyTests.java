package ai.lzy.service.idempotency;

import ai.lzy.service.Graphs;
import ai.lzy.service.WithoutWbAndSchedulerLzyContextTests;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.workflow.LWFPS;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import static ai.lzy.service.IamUtils.authorize;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static org.junit.Assert.assertEquals;

public class IdempotencyTests extends WithoutWbAndSchedulerLzyContextTests {
    private static final String workflowName = "test-workflow";
    private static final String reason = "no-matter";

    private static final String startWfIdk = "start_workflow_" + workflowName;
    private static final String stopWfIdk = "stop_workflow_" + workflowName;
    private static final String execGraphIdk = "exec_graph_" + workflowName;
    private static final String stopGraphIdk = "stop_graph_" + workflowName;

    private String executionId;
    private LMST.StorageConfig storage;

    private LzyWorkflowServiceBlockingStub authLzyClient;

    private LMST.StorageConfig createStorage() {
        return authLzyClient.getOrCreateDefaultStorage(
            LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build()).getStorage();
    }

    private String startWorkflow(LMST.StorageConfig storage) {
        var request = LWFS.StartWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setSnapshotStorage(storage)
            .build();
        return withIdempotencyKey(authLzyClient, startWfIdk).startWorkflow(request).getExecutionId();
    }

    private void finishWorkflow(String executionId) {
        var request = LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setReason(reason)
            .build();
        //noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(authLzyClient, stopWfIdk).finishWorkflow(request);
    }

    private void abortWorkflow(String executionId) {
        var request = LWFS.AbortWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setReason(reason)
            .build();
        //noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(authLzyClient, stopWfIdk).abortWorkflow(request);
    }

    private void privateAbortWorkflow(String executionId) {
        var request = LWFS.AbortWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setReason(reason)
            .build();
        //noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(lzyPrivateClient, stopWfIdk).abortWorkflow(request);
    }

    private String executeGraph(String executionId, LMST.StorageConfig storage) {
        var request = LWFS.ExecuteGraphRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraph(Graphs.simpleGraph(storage))
            .build();
        return withIdempotencyKey(authLzyClient, execGraphIdk).executeGraph(request).getGraphId();
    }

    private void stopGraph(String executionId, String graphId) {
        var request = LWFS.StopGraphRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraphId(graphId)
            .build();
        //noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(authLzyClient, stopGraphIdk).stopGraph(request);
    }

    @Before
    public void before() throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, InterruptedException {
        authLzyClient = authorize(lzyClient, "test-user-1", iamClient);
        storage = createStorage();
        executionId = startWorkflow(storage);
    }

    @After
    public void after() {
        try {
            finishWorkflow(executionId);
        } catch (StatusRuntimeException sre) {
            // intentionally blank
        }
    }

    @Test
    public void startWorkflowIdempotentCall() {
        assertEquals(startWorkflow(storage), startWorkflow(storage));
    }

    @Test
    public void finishWorkflowIdempotentCall() {
        finishWorkflow(executionId);
        finishWorkflow(executionId);
    }

    @Test
    public void abortWorkflowIdempotentCall() {
        abortWorkflow(executionId);
        abortWorkflow(executionId);
    }

    @Test
    public void privateAbortWorkflowIdempotentCall() {
        privateAbortWorkflow(executionId);
        privateAbortWorkflow(executionId);
    }

    @Test
    public void executeGraphIdempotentCall() {
        assertEquals(executeGraph(executionId, storage), executeGraph(executionId, storage));
    }

    @Test
    public void stopGraphIdempotentCall() {
        var graphId = executeGraph(executionId, storage);
        stopGraph(executionId, graphId);
        stopGraph(executionId, graphId);
    }
}
