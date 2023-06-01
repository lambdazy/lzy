package ai.lzy.service.auth;

import ai.lzy.allocator.test.BaseTestWithAllocator;
import ai.lzy.channelmanager.test.BaseTestWithChannelManager;
import ai.lzy.graph.test.BaseTestWithGraphExecutor;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.service.TestContextConfigurator;
import ai.lzy.service.test.BaseTestWithLzy;
import ai.lzy.storage.test.BaseTestWithStorage;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import java.sql.SQLException;

import static ai.lzy.util.auth.credentials.JwtUtils.invalidCredentials;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class WrongCredentialsTests {
    private static final BaseTestWithIam iamTestContext = new BaseTestWithIam();
    private static final BaseTestWithStorage storageTestContext = new BaseTestWithStorage();
    private static final BaseTestWithChannelManager channelManagerTestContext = new BaseTestWithChannelManager();
    private static final BaseTestWithGraphExecutor graphExecutorTestContext = new BaseTestWithGraphExecutor();
    private static final BaseTestWithAllocator allocatorTestContext = new BaseTestWithAllocator();
    private static final BaseTestWithLzy lzyServiceTestContext = new BaseTestWithLzy();

    @ClassRule
    public static PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @ClassRule
    public static PreparedDbRule storageDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @ClassRule
    public static PreparedDbRule channelManagerDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @ClassRule
    public static PreparedDbRule graphExecutorDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @ClassRule
    public static PreparedDbRule allocatorDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @ClassRule
    public static PreparedDbRule lzyServiceDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private static final String workflowName = "test-workflow";
    private static final String executionId = "test-execution-id";
    private static final String graphId = "test-graph-id";
    private static final String reason = "no-matter";

    private static LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub wrongAuthLzyGrpcClient;

    @BeforeClass
    public static void beforeClass() throws Exception {
        TestContextConfigurator.setUp(
            iamTestContext, iamDb.getConnectionInfo(),
            storageTestContext, storageDb.getConnectionInfo(),
            channelManagerTestContext, channelManagerDb.getConnectionInfo(),
            graphExecutorTestContext, graphExecutorDb.getConnectionInfo(),
            allocatorTestContext, allocatorDb.getConnectionInfo(),
            lzyServiceTestContext, lzyServiceDb.getConnectionInfo()
        );

        wrongAuthLzyGrpcClient = lzyServiceTestContext.getGrpcClient().withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, invalidCredentials("user", "GITHUB")::token)
        );
    }

    @AfterClass
    public static void afterClass() throws InterruptedException, SQLException {
        TestContextConfigurator.tearDown(
            iamTestContext,
            storageTestContext,
            channelManagerTestContext,
            graphExecutorTestContext,
            allocatorTestContext,
            lzyServiceTestContext
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
        //noinspection ResultOfMethodCallIgnored
        doPermDeniedAssert(() -> wrongAuthLzyGrpcClient.readStdSlots(request));
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
