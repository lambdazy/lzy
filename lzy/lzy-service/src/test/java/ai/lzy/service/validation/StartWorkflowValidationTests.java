package ai.lzy.service.validation;

import ai.lzy.allocator.test.BaseTestWithAllocator;
import ai.lzy.channelmanager.test.BaseTestWithChannelManager;
import ai.lzy.graph.test.BaseTestWithGraphExecutor;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.service.TestContextConfigurator;
import ai.lzy.service.ValidationTests;
import ai.lzy.service.test.BaseTestWithLzy;
import ai.lzy.storage.test.BaseTestWithStorage;
import ai.lzy.v1.common.LMST;
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

public class StartWorkflowValidationTests implements ValidationTests<LWFS.StartWorkflowRequest> {
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
            lzyServiceTestContext.getGrpcClient(), "test-user-1", iamTestContext.iamSubjectsClient()
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
    public void missingWorkflowName() {
        var request = LWFS.StartWorkflowRequest.newBuilder()
            .setSnapshotStorage(LMST.StorageConfig.getDefaultInstance())
            .build();
        doAssert(request);
    }

    @Test
    public void missingStorageConfig() {
        doAssert(LWFS.StartWorkflowRequest.newBuilder().setWorkflowName("test-workflow").build());
    }

    @Override
    public ThrowingRunnable action(LWFS.StartWorkflowRequest request) {
        //noinspection ResultOfMethodCallIgnored
        return () -> authLzyGrpcClient.startWorkflow(request);
    }
}
