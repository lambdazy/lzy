package ai.lzy.service.auth;

import ai.lzy.allocator.test.BaseTestWithAllocator;
import ai.lzy.channelmanager.test.BaseTestWithChannelManager;
import ai.lzy.graph.test.BaseTestWithGraphExecutor;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.service.TestContextConfigurator;
import ai.lzy.service.test.BaseTestWithLzy;
import ai.lzy.storage.test.BaseTestWithStorage;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.SQLException;

import static ai.lzy.service.IamUtils.authorize;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class OthersWorkflowPermissionTests {
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
    private static String executionId;
    private static final String graphId = "test-graph-id";
    private static final String reason = "no-matter";

    private static LzyWorkflowServiceBlockingStub authGrpcClient1;
    private static LzyWorkflowServiceBlockingStub authGrpcClient2;

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

        authGrpcClient1 = authorize(
            lzyServiceTestContext.getGrpcClient(), "test-user-1", iamTestContext.iamSubjectsClient()
        );
        authGrpcClient2 = authorize(
            lzyServiceTestContext.getGrpcClient(), "test-user-2", iamTestContext.iamSubjectsClient()
        );

        executionId = authGrpcClient1.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setSnapshotStorage(LMST.StorageConfig.getDefaultInstance())
            .build()).getExecutionId();
    }

    @AfterClass
    public static void afterClass() throws SQLException, InterruptedException {
        //noinspection ResultOfMethodCallIgnored
        authGrpcClient1.finishWorkflow(
            LWFS.FinishWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .build());

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
    public void finishOthersWorkflowForbidden() {
        var request = LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setReason(reason)
            .build();

        //noinspection ResultOfMethodCallIgnored
        var sre = assertThrows(StatusRuntimeException.class, () -> authGrpcClient2.finishWorkflow(request));
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
        var sre = assertThrows(StatusRuntimeException.class, () -> authGrpcClient2.abortWorkflow(request));
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
        var sre = assertThrows(StatusRuntimeException.class, () -> authGrpcClient2.executeGraph(request));
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
        var sre = assertThrows(StatusRuntimeException.class, () -> authGrpcClient2.stopGraph(request));
        assertSame(Status.INVALID_ARGUMENT.getCode(), sre.getStatus().getCode());
    }

    @Test
    public void readStdSlotsOtherUsersForbidden() {
        var request = LWFS.ReadStdSlotsRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .build();

        //noinspection ResultOfMethodCallIgnored
        var sre = assertThrows(StatusRuntimeException.class, () -> authGrpcClient2.readStdSlots(request));
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
}
