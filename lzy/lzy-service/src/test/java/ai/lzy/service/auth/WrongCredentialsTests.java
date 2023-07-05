package ai.lzy.service.auth;

import ai.lzy.service.IamOnlyLzyContextTests;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Before;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import static ai.lzy.util.auth.credentials.JwtUtils.invalidCredentials;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class WrongCredentialsTests extends IamOnlyLzyContextTests {
    private static final String workflowName = "test-workflow";
    private static final String executionId = "test-execution-id";
    private static final String graphId = "test-graph-id";
    private static final String reason = "no-matter";

    private LzyWorkflowServiceBlockingStub wrongAuthLzyGrpcClient;

    @Before
    public void before() {
        wrongAuthLzyGrpcClient = lzyClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, invalidCredentials("user", "GITHUB")::token)
        );
    }

    @Test
    public void startWorkflowWithWrongUserCredentials() {
        var request = LWFS.StartWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setSnapshotStorage(LMST.StorageConfig.getDefaultInstance())
            .build();
        //noinspection ResultOfMethodCallIgnored
        doPermDeniedAssert(() -> wrongAuthLzyGrpcClient.startWorkflow(request));
    }

    @Test
    public void finishWorkflowWithWrongUserCredentials() {
        var request = LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setReason(reason)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doPermDeniedAssert(() -> wrongAuthLzyGrpcClient.finishWorkflow(request));
    }

    @Test
    public void abortWorkflowWithWrongUserCredentials() {
        var request = LWFS.AbortWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setReason(reason)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doPermDeniedAssert(() -> wrongAuthLzyGrpcClient.abortWorkflow(request));
    }

    @Test
    public void executeGraphWithWrongUserCredentials() {
        var request = LWFS.ExecuteGraphRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraph(LWF.Graph.getDefaultInstance())
            .build();
        //noinspection ResultOfMethodCallIgnored
        doPermDeniedAssert(() -> wrongAuthLzyGrpcClient.executeGraph(request));
    }

    @Test
    public void graphStatusWithWrongUserCredentials() {
        var request = LWFS.GraphStatusRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraphId(graphId)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doPermDeniedAssert(() -> wrongAuthLzyGrpcClient.graphStatus(request));
    }

    @Test
    public void stopGraphWithWrongUserCredentials() {
        var request = LWFS.StopGraphRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraphId(graphId)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doPermDeniedAssert(() -> wrongAuthLzyGrpcClient.stopGraph(request));
    }

    @Test
    public void readStdSlotsWithWrongUserCredentials() {
        var request = LWFS.ReadStdSlotsRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .build();

        doPermDeniedAssert(() -> wrongAuthLzyGrpcClient.readStdSlots(request).next());
    }

    @Test
    public void getVmPoolsWithWrongUserCredentials() {
        var request = LWFS.GetAvailablePoolsRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doPermDeniedAssert(() -> wrongAuthLzyGrpcClient.getAvailablePools(request));
    }

    @Test
    public void createS3WithWrongUserCredentials() {
        var request = LWFS.GetOrCreateDefaultStorageRequest.getDefaultInstance();
        //noinspection ResultOfMethodCallIgnored
        doPermDeniedAssert(() -> wrongAuthLzyGrpcClient.getOrCreateDefaultStorage(request));
    }

    private static void doPermDeniedAssert(ThrowingRunnable action) {
        var sre = assertThrows(StatusRuntimeException.class, action);
        assertSame(Status.PERMISSION_DENIED.getCode(), sre.getStatus().getCode());
    }
}
