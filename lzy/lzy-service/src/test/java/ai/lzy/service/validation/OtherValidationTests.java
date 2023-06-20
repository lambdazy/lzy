package ai.lzy.service.validation;

import ai.lzy.allocator.test.BaseTestWithAllocator;
import ai.lzy.channelmanager.test.BaseTestWithChannelManager;
import ai.lzy.graph.test.BaseTestWithGraphExecutor;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.service.TestContextConfigurator;
import ai.lzy.service.ValidationTests;
import ai.lzy.service.test.LzyServiceTestContext;
import ai.lzy.storage.test.BaseTestWithStorage;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.*;
import org.junit.function.ThrowingRunnable;

import java.sql.SQLException;

import static ai.lzy.service.IamUtils.authorize;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

@Ignore
public class OtherValidationTests {
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
    private static LzyWorkflowServiceBlockingStub authLzyGrpcClient;

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

        authLzyGrpcClient = authorize(
            lzyServiceTestContext.grpcClient(), "test-user-1", iamTestContext.iamSubjectsClient()
        );
        executionId = ValidationTests.startWorkflow(authLzyGrpcClient, workflowName);
    }

    @AfterClass
    public static void afterClass() throws InterruptedException, SQLException {
        ValidationTests.finishWorkflow(authLzyGrpcClient, workflowName, executionId);

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
    public void missingWorkflowNameInReadStdRequest() {
        var request = LWFS.ReadStdSlotsRequest.newBuilder().setExecutionId(executionId).build();
        doAssert(() -> authLzyGrpcClient.readStdSlots(request).next());
    }

    @Test
    public void missingExecutionIdInReadStdRequest() {
        var request = LWFS.ReadStdSlotsRequest.newBuilder().setWorkflowName(workflowName).build();
        doAssert(() -> authLzyGrpcClient.readStdSlots(request).next());
    }

    @Test
    public void invalidWorkflowNameInReadStdSlots() {
        var request = LWFS.ReadStdSlotsRequest.newBuilder()
            .setWorkflowName("unknown-workflow-name")
            .setExecutionId(executionId)
            .build();
        doAssert(() -> authLzyGrpcClient.readStdSlots(request).next());
    }

    @Test
    public void invalidExecutionIdInReadStdSlots() {
        var request = LWFS.ReadStdSlotsRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId("unknown-exec-id")
            .build();
        doAssert(() -> authLzyGrpcClient.readStdSlots(request).next());
    }

    @Test
    public void missingWorkflowNameInGetVmPoolsRequest() {
        var request = LWFS.GetAvailablePoolsRequest.newBuilder().setExecutionId(executionId).build();
        //noinspection ResultOfMethodCallIgnored
        doAssert(() -> authLzyGrpcClient.getAvailablePools(request));
    }

    @Test
    public void missingExecutionIdInGetVmPoolsRequest() {
        var request = LWFS.GetAvailablePoolsRequest.newBuilder().setWorkflowName(workflowName).build();
        //noinspection ResultOfMethodCallIgnored
        doAssert(() -> authLzyGrpcClient.getAvailablePools(request));
    }

    @Test
    public void invalidWorkflowNameInGetVmPoolsRequest() {
        var request = LWFS.GetAvailablePoolsRequest.newBuilder()
            .setWorkflowName("unknown-workflow-name")
            .setExecutionId(executionId)
            .build();
        //noinspection ResultOfMethodCallIgnored
        doAssert(() -> authLzyGrpcClient.getAvailablePools(request));
    }

    @Test
    public void invalidExecutionIdInGetVmPoolsRequest() {
        var request = LWFS.GetAvailablePoolsRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId("unknown-exec-id")
            .build();
        //noinspection ResultOfMethodCallIgnored
        doAssert(() -> authLzyGrpcClient.getAvailablePools(request));
    }

    private static void doAssert(ThrowingRunnable action) {
        var sre = assertThrows(StatusRuntimeException.class, action);
        assertSame(Status.INVALID_ARGUMENT.getCode(), sre.getStatus().getCode());
    }
}
