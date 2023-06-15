package ai.lzy.service.validation;

import ai.lzy.allocator.test.BaseTestWithAllocator;
import ai.lzy.channelmanager.test.BaseTestWithChannelManager;
import ai.lzy.graph.test.BaseTestWithGraphExecutor;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.service.TestContextConfigurator;
import ai.lzy.service.ValidationTests;
import ai.lzy.service.test.LzyServiceTestContext;
import ai.lzy.storage.test.BaseTestWithStorage;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import java.sql.SQLException;

import static ai.lzy.service.IamUtils.authorize;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class FinishExecutionValidationTests implements ValidationTests<LWFS.FinishWorkflowRequest> {
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
    private static final String reason = "no-matter";
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
    public void missingWorkflowName() {
        var request = LWFS.FinishWorkflowRequest.newBuilder()
            .setExecutionId(executionId)
            .setReason(reason)
            .build();
        doAssert(request);
    }

    @Test
    public void missingExecutionId() {
        var request = LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setReason(reason)
            .build();
        doAssert(request);
    }

    @Test
    public void missingFinishReason() {
        var request = LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .build();
        doAssert(request);
    }

    @Test
    public void invalidWorkflowName() {
        var request = LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName("unknown-workflow-name")
            .setExecutionId(executionId)
            .setReason(reason)
            .build();
        doAssert(request);
    }

    @Test
    public void invalidExecutionId() {
        var request = LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId("unknown-exec-id")
            .setReason(reason)
            .build();
        doAssert(request);
    }

    @Override
    public ThrowingRunnable action(LWFS.FinishWorkflowRequest request) {
        //noinspection ResultOfMethodCallIgnored
        return () -> withIdempotencyKey(authLzyGrpcClient, "finish_wf").finishWorkflow(request);
    }
}
