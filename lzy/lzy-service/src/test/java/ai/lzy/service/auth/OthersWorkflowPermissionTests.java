package ai.lzy.service.auth;

import ai.lzy.allocator.test.BaseTestWithAllocator;
import ai.lzy.channelmanager.test.BaseTestWithChannelManager;
import ai.lzy.graph.test.BaseTestWithGraphExecutor;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.service.TestContextConfigurator;
import ai.lzy.service.ValidationTests;
import ai.lzy.service.test.LzyServiceTestContext;
import ai.lzy.storage.test.BaseTestWithStorage;
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
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class OthersWorkflowPermissionTests {
    private static final BaseTestWithIam iamTestContext = new BaseTestWithIam();
    private static final BaseTestWithStorage storageTestContext = new BaseTestWithStorage();
    private static final BaseTestWithChannelManager channelManagerTestContext = new BaseTestWithChannelManager();
    private static final BaseTestWithGraphExecutor graphExecutorTestContext = new BaseTestWithGraphExecutor();
    private static final BaseTestWithAllocator allocatorTestContext = new BaseTestWithAllocator();
    private static final LzyServiceTestContext lzyServiceTestContext = new LzyServiceTestContext();

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
            lzyServiceTestContext.grpcClient(), "test-user-1", iamTestContext.iamSubjectsClient()
        );
        authGrpcClient2 = authorize(
            lzyServiceTestContext.grpcClient(), "test-user-2", iamTestContext.iamSubjectsClient()
        );

        executionId = ValidationTests.startWorkflow(authGrpcClient1, workflowName);
    }

    @AfterClass
    public static void afterClass() throws SQLException, InterruptedException {
        ValidationTests.finishWorkflow(authGrpcClient1, workflowName, executionId);

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
