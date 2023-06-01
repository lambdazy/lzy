package ai.lzy.service;

import ai.lzy.allocator.test.AllocatorServiceDecorator;
import ai.lzy.allocator.test.BaseTestWithAllocator;
import ai.lzy.channelmanager.test.BaseTestWithChannelManager;
import ai.lzy.graph.test.BaseTestWithGraphExecutor;
import ai.lzy.graph.test.GraphExecutorDecorator;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.iam.test.LzySubjectServiceDecorator;
import ai.lzy.service.test.BaseTestWithLzy;
import ai.lzy.storage.test.BaseTestWithStorage;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc.LzyWorkflowPrivateServiceBlockingStub;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.sql.SQLException;

import static ai.lzy.service.IamUtils.authorize;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;

public abstract class ContextAwareTests {
    private final BaseTestWithIam iamTestContext = new BaseTestWithIam();
    private final BaseTestWithStorage storageTestContext = new BaseTestWithStorage();
    private final BaseTestWithChannelManager channelManagerTestContext = new BaseTestWithChannelManager();
    private final BaseTestWithGraphExecutor graphExecutorTestContext = new BaseTestWithGraphExecutor();
    private final BaseTestWithAllocator allocatorTestContext = new BaseTestWithAllocator();
    private final BaseTestWithLzy lzyServiceTestContext = new BaseTestWithLzy();

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule storageDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule channelManagerDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule graphExecutorDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule allocatorDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule lzyServiceDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    protected LzyWorkflowServiceBlockingStub authLzyGrpcClient;
    protected LzyWorkflowPrivateServiceBlockingStub authLzyPrivateGrpcClient;

    @Before
    public void before() throws Exception {
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

        var internalUserCredentials = iamTestContext.getClientConfig().createRenewableToken();
        authLzyPrivateGrpcClient = newBlockingClient(
            lzyServiceTestContext.getPrivateGrpcClient(), "TestClient", () -> internalUserCredentials.get().token()
        );
    }

    @After
    public void after() throws InterruptedException, SQLException {
        TestContextConfigurator.tearDown(
            iamTestContext,
            storageTestContext,
            channelManagerTestContext,
            graphExecutorTestContext,
            allocatorTestContext,
            lzyServiceTestContext
        );
    }

    public AllocatorServiceDecorator allocator() {
        return allocatorTestContext.allocator();
    }

    public LzySubjectServiceDecorator iamSubjectsService() {
        return iamTestContext.subjectsService();
    }

    public GraphExecutorDecorator graphExecutor() {
        return graphExecutorTestContext.getGraphExecutor();
    }
}
