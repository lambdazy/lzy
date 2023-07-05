package ai.lzy.service.auth;

import ai.lzy.service.IamOnlyLzyContextTests;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class UnauthenticatedLzyClientTests extends IamOnlyLzyContextTests {
    private static final String workflowName = "test-workflow";
    private static final String executionId = "test-execution-id";
    private static final String graphId = "test-graph-id";
    private static final String reason = "no-matter";

    @Test
    public void startWorkflowWithUnauthClient() {
        var request = LWFS.StartWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setSnapshotStorage(LMST.StorageConfig.getDefaultInstance())
            .build();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> lzyClient.startWorkflow(request));
    }

    @Test
    public void finishWorkflowWithUnauthClient() {
        var request = LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setReason(reason)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> lzyClient.finishWorkflow(request));
    }

    @Test
    public void abortWorkflowWithUnauthClient() {
        var request = LWFS.AbortWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setReason(reason)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> lzyClient.abortWorkflow(request));
    }

    @Test
    public void executeGraphWithUnauthClient() {
        var request = LWFS.ExecuteGraphRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraph(LWF.Graph.getDefaultInstance())
            .build();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> lzyClient.executeGraph(request));
    }

    @Test
    public void graphStatusWithUnauthClient() {
        var request = LWFS.GraphStatusRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraphId(graphId)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> lzyClient.graphStatus(request));
    }

    @Test
    public void stopGraphWithUnauthClient() {
        var request = LWFS.StopGraphRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraphId(graphId)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> lzyClient.stopGraph(request));
    }

    @Test
    public void readStdSlotsWithUnauthClient() {
        var request = LWFS.ReadStdSlotsRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .build();
        doUnauthAssert(() -> lzyClient.readStdSlots(request).next());
    }

    @Test
    public void getVmPoolsWithUnauthClient() {
        var request = LWFS.GetAvailablePoolsRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> lzyClient.getAvailablePools(request));
    }

    @Test
    public void createS3WithUnauthClient() {
        var request = LWFS.GetOrCreateDefaultStorageRequest.getDefaultInstance();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> lzyClient.getOrCreateDefaultStorage(request));
    }

    private static void doUnauthAssert(ThrowingRunnable action) {
        var sre = assertThrows(StatusRuntimeException.class, action);
        assertSame(Status.UNAUTHENTICATED.getCode(), sre.getStatus().getCode());
    }
}
