package ai.lzy.service;

import ai.lzy.allocator.test.AllocatorProxy;
import ai.lzy.allocator.test.BaseTestWithAllocator;
import ai.lzy.channelmanager.test.BaseTestWithChannelManager;
import ai.lzy.channelmanager.test.ChannelManagerDecorator;
import ai.lzy.graph.test.BaseTestWithGraphExecutor;
import ai.lzy.graph.test.GraphExecutorDecorator;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.User;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.longrunning.OperationsService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.util.ClientVersionInterceptor;
import ai.lzy.service.workflow.WorkflowService;
import ai.lzy.storage.test.BaseTestWithStorage;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.auth.exceptions.AuthPermissionDeniedException;
import ai.lzy.util.auth.exceptions.AuthUnauthenticatedException;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.graph.GraphExecutorApi.GraphExecuteRequest;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import com.google.common.net.HostAndPort;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.jsonwebtoken.Claims;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class BaseTest {
    private static final BaseTestWithIam iamTestContext = new BaseTestWithIam();
    private static final BaseTestWithStorage storageTestContext = new BaseTestWithStorage();
    private static final BaseTestWithChannelManager channelManagerTestContext = new BaseTestWithChannelManager();
    private static final BaseTestWithGraphExecutor graphExecutorTestContext = new BaseTestWithGraphExecutor();
    protected static final BaseTestWithAllocator allocatorTestContext = new BaseTestWithAllocator();

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

    protected ApplicationContext context;
    protected LzyServiceConfig config;

    private Server lzyServer;

    protected ManagedChannel lzyServiceChannel;
    protected LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub unauthorizedWorkflowClient;
    protected LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub authorizedWorkflowClient;

    protected LongRunningServiceGrpc.LongRunningServiceBlockingStub operationServiceClient;

    protected RenewableJwt internalUserCredentials;
    protected AuthServerInterceptor authInterceptor;
    protected LzyServiceMetrics metrics;

    @Before
    public void setUp() throws IOException, InterruptedException {
        ClientVersionInterceptor.DISABLE_VERSION_CHECK.set(true);
        var iamDbConfig = preparePostgresConfig("iam", iamDb.getConnectionInfo());
        iamTestContext.setUp(iamDbConfig);
        var iamAddress = "localhost:" + iamTestContext.getPort();

        var storageCfgOverrides = preparePostgresConfig("storage", storageDb.getConnectionInfo());
        storageCfgOverrides.put("storage.iam.address", iamAddress);
        storageTestContext.setUp(storageCfgOverrides);

        var channelManagerCfgOverrides = preparePostgresConfig("channel-manager", channelManagerDb.getConnectionInfo());
        channelManagerCfgOverrides.put("channel-manager.iam.address", iamAddress);
        channelManagerTestContext.setUp(channelManagerCfgOverrides);

        var allocatorCfgOverrides = preparePostgresConfig("allocator", allocatorDb.getConnectionInfo());
        allocatorCfgOverrides.put("allocator.thread-allocator.enabled", true);
        allocatorCfgOverrides.put("allocator.thread-allocator.vm-class-name", "ai.lzy.portal.App");
        allocatorCfgOverrides.put("allocator.iam.address", iamAddress);
        allocatorTestContext.setUp(allocatorCfgOverrides);

        var graphExecCfgOverrides = preparePostgresConfig("graph-executor", graphExecutorDb.getConnectionInfo());
        graphExecCfgOverrides.put("graph-executor.iam.address", iamAddress);
        graphExecutorTestContext.setUp(graphExecCfgOverrides);

        WorkflowService.PEEK_RANDOM_PORTAL_PORTS = true;  // To recreate portals for all wfs

        var lzyConfigOverrides = preparePostgresConfig("lzy-service", lzyServiceDb.getConnectionInfo());

        context = ApplicationContext.run(PropertySource.of(lzyConfigOverrides), "test-mock");
        config = context.getBean(LzyServiceConfig.class);
        config.getIam().setAddress(iamAddress);

        config.setGraphExecutorAddress("localhost:" + graphExecutorTestContext.getPort());
        config.getStorage().setAddress("localhost:" + storageTestContext.getPort());

        authInterceptor = new AuthServerInterceptor(credentials -> {
            var iam = config.getIam();

            var user = Objects.requireNonNull(JwtUtils.parseJwt(credentials.token())).get(Claims.ISSUER);

            if (user == null) {
                throw new AuthUnauthenticatedException("heck");
            }
            if (!iam.getInternalUserName().equals(user)) {
                throw new AuthPermissionDeniedException("heck");
            }

            return new User(iam.getInternalUserName(), AuthProvider.INTERNAL, iam.getInternalUserName());
        });

        var workflowAddress = HostAndPort.fromString(config.getAddress());

        var lzyServiceOpDao = context.getBean(OperationDao.class, Qualifiers.byName("LzyServiceOperationDao"));
        var versionInterceptor = context.getBean(ClientVersionInterceptor.class);

        var opService = new OperationsService(lzyServiceOpDao);

        lzyServer = App.createServer(workflowAddress, versionInterceptor, authInterceptor,
            context.getBean(LzyService.class), context.getBean(LzyServicePrivateApi.class), opService);
        lzyServer.start();

        lzyServiceChannel = newGrpcChannel(workflowAddress, LzyWorkflowServiceGrpc.SERVICE_NAME);
        unauthorizedWorkflowClient = LzyWorkflowServiceGrpc.newBlockingStub(lzyServiceChannel);

        internalUserCredentials = config.getIam().createRenewableToken();
        authorizedWorkflowClient = newBlockingClient(unauthorizedWorkflowClient, "TestClient",
            () -> internalUserCredentials.get().token());

        operationServiceClient = newBlockingClient(LongRunningServiceGrpc.newBlockingStub(lzyServiceChannel),
            "TestClient", () -> internalUserCredentials.get().token());

        metrics = context.getBean(LzyServiceMetrics.class);
    }

    @After
    public void tearDown() throws SQLException, InterruptedException, DaoException {
        WorkflowService.PEEK_RANDOM_PORTAL_PORTS = false;
        lzyServiceChannel.shutdown();
        lzyServer.shutdown();
        lzyServer.awaitTermination();
        graphExecutorTestContext.after();
        //schedulerTestContext.after();
        channelManagerTestContext.after();
        allocatorTestContext.after();
        storageTestContext.after();
        iamTestContext.after();
        context.stop();
    }

    protected void shutdownStorage() throws InterruptedException {
        storageTestContext.after();
    }

    public static String buildSlotUri(String key, LMST.StorageConfig storageConfig) {
        return storageConfig.getUri() + "/" + key;
    }

    protected void onExecuteGraph(Consumer<GraphExecuteRequest> action) {
        var graphExecutor = graphExecutorTestContext.getContext().getBean(GraphExecutorDecorator.class);
        graphExecutor.setOnExecute(action);
    }

    protected void onStopGraph(Consumer<String> action) {
        var graphExecutor = graphExecutorTestContext.getContext().getBean(GraphExecutorDecorator.class);
        graphExecutor.setOnStop(action);
    }

    protected void onDeleteSession(Runnable action) {
        var allocator = allocatorTestContext.getContext().getBean(AllocatorProxy.class);
        allocator.setOnDeleteSession(action);
    }

    protected void onFreeVm(Runnable action) {
        var allocator = allocatorTestContext.getContext().getBean(AllocatorProxy.class);
        allocator.setOnFree(action);
    }

    protected void onChannelsDestroy(Consumer<String> action) {
        var channelManager = channelManagerTestContext.getContext().getBean(ChannelManagerDecorator.class);
        channelManager.setOnDestroyAll(action);
    }

    protected LWFS.StartWorkflowResponse startWorkflow(String name, LMST.StorageConfig storageConfig) {
        return authorizedWorkflowClient.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
            .setWorkflowName(name).setSnapshotStorage(storageConfig).build());
    }

    protected LWFS.FinishWorkflowResponse finishWorkflow(String name, String activeExecutionId)
        throws InvalidProtocolBufferException
    {
        var finishOp = authorizedWorkflowClient.finishWorkflow(
            LWFS.FinishWorkflowRequest.newBuilder()
                .setWorkflowName(name)
                .setExecutionId(activeExecutionId)
                .build());
        finishOp = awaitOperationDone(operationServiceClient, finishOp.getId(), Duration.ofSeconds(10));
        return finishOp.getResponse().unpack(LWFS.FinishWorkflowResponse.class);
    }
}
