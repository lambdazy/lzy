package ai.lzy.service.auth;

import ai.lzy.allocator.test.BaseTestWithAllocator;
import ai.lzy.channelmanager.test.BaseTestWithChannelManager;
import ai.lzy.graph.test.BaseTestWithGraphExecutor;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.service.TestContextConfigurator;
import ai.lzy.service.test.LzyServiceTestContext;
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
import org.junit.function.ThrowingRunnable;

import java.sql.SQLException;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class UnauthenticatedLzyClientTests {
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
    private static final String executionId = "test-execution-id";
    private static final String graphId = "test-graph-id";
    private static final String reason = "no-matter";

    private static LzyWorkflowServiceBlockingStub unauthLzyGrpcClient;

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

        unauthLzyGrpcClient = lzyServiceTestContext.grpcClient();
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
    public void startWorkflowWithUnauthClient() {
        var request = LWFS.StartWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setSnapshotStorage(LMST.StorageConfig.getDefaultInstance())
            .build();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> unauthLzyGrpcClient.startWorkflow(request));
    }

    @Test
    public void finishWorkflowWithUnauthClient() {
        var request = LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setReason(reason)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> unauthLzyGrpcClient.finishWorkflow(request));
    }

    @Test
    public void abortWorkflowWithUnauthClient() {
        var request = LWFS.AbortWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setReason(reason)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> unauthLzyGrpcClient.abortWorkflow(request));
    }

    @Test
    public void executeGraphWithUnauthClient() {
        var request = LWFS.ExecuteGraphRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraph(LWF.Graph.getDefaultInstance())
            .build();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> unauthLzyGrpcClient.executeGraph(request));
    }

    @Test
    public void graphStatusWithUnauthClient() {
        var request = LWFS.GraphStatusRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraphId(graphId)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> unauthLzyGrpcClient.graphStatus(request));
    }

    @Test
    public void stopGraphWithUnauthClient() {
        var request = LWFS.StopGraphRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraphId(graphId)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> unauthLzyGrpcClient.stopGraph(request));
    }

    @Test
    public void readStdSlotsWithUnauthClient() {
        var request = LWFS.ReadStdSlotsRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .build();
        doUnauthAssert(() -> unauthLzyGrpcClient.readStdSlots(request).next());
    }

    @Test
    public void getVmPoolsWithUnauthClient() {
        var request = LWFS.GetAvailablePoolsRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> unauthLzyGrpcClient.getAvailablePools(request));
    }

    @Test
    public void createS3WithUnauthClient() {
        var request = LWFS.GetOrCreateDefaultStorageRequest.getDefaultInstance();
        //noinspection ResultOfMethodCallIgnored
        doUnauthAssert(() -> unauthLzyGrpcClient.getOrCreateDefaultStorage(request));
    }

    private static void doUnauthAssert(ThrowingRunnable action) {
        var sre = assertThrows(StatusRuntimeException.class, action);
        assertSame(Status.UNAUTHENTICATED.getCode(), sre.getStatus().getCode());
    }
}
