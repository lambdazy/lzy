package ai.lzy.service;

import ai.lzy.allocator.test.AllocatorContextImpl;
import ai.lzy.allocator.test.AllocatorServiceDecorator;
import ai.lzy.channelmanager.test.ChannelManagerContextImpl;
import ai.lzy.graph.test.GraphExecutorContextImpl;
import ai.lzy.graph.test.GraphExecutorDecorator;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.test.IamContextImpl;
import ai.lzy.iam.test.LzySubjectServiceDecorator;
import ai.lzy.longrunning.dao.OperationDaoDecorator;
import ai.lzy.service.test.LzyServiceContextImpl;
import ai.lzy.service.util.ClientVersionInterceptor;
import ai.lzy.storage.test.StorageContextImpl;
import ai.lzy.test.context.LzyInThread;
import ai.lzy.test.context.config.LzyConfig;
import ai.lzy.util.grpc.RequestIdInterceptor;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc.LzyWorkflowPrivateServiceBlockingStub;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.zonky.test.db.postgres.embedded.ConnectionInfo;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.*;

public abstract class WithoutWbAndSchedulerLzyContextTests {
    protected static final String CLIENT_NAME = "TestClient";

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule allocatorDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule channelManagerDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule graphExecutorDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule storageDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule lzyServiceDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    public LzyInThread lzy = new LzyInThread();

    private ManagedChannel iamServiceGrpcChannel;
    private ManagedChannel lzyServiceGrpcChannel;

    protected LzyWorkflowServiceBlockingStub lzyClient;
    protected LzyWorkflowPrivateServiceBlockingStub unauthLzyPrivateClient;
    protected LzyWorkflowPrivateServiceBlockingStub lzyPrivateClient;
    protected SubjectServiceGrpcClient iamClient;

    public static String prepareDbUrl(ConnectionInfo ci) {
        return "jdbc:postgresql://localhost:%d/%s".formatted(ci.getPort(), ci.getDbName());
    }

    public OperationDaoDecorator lzyServiceOperations() {
        return lzy.micronautContext().getBean(LzyServiceContextImpl.class).operationsDao();
    }

    public AllocatorServiceDecorator allocator() {
        return lzy.micronautContext().getBean(AllocatorContextImpl.class).allocator();
    }

    public LzySubjectServiceDecorator iamSubjectsService() {
        return lzy.micronautContext().getBean(IamContextImpl.class).subjectsService();
    }

    public GraphExecutorDecorator graphExecutor() {
        return lzy.micronautContext().getBean(GraphExecutorContextImpl.class).getMicronautContext()
            .getBean(GraphExecutorDecorator.class);
    }

    @Before
    public final void setUp() throws Exception {
        ClientVersionInterceptor.DISABLE_VERSION_CHECK.set(true);

        var configs = LzyConfig.Configs.builder()
            .setIamConfig("../iam/src/main/resources/application-test.yml")
            .setAllocatorConfig("../allocator/src/main/resources/application-test.yml")
            .setChannelManagerConfig("../channel-manager/src/main/resources/application-test.yml")
            .setGraphExecutorConfig("../graph-executor/src/main/resources/application-test.yml")
            .setStorageConfig("../storage/src/main/resources/application-test.yml")
            .setLzyServiceConfig("../lzy-service/src/main/resources/application-test.yml")
            .build();

        var configOverrides = Map.<String, Object>of(
            "allocator.thread-allocator.enabled", true,
            "allocator.thread-allocator.vm-class-name", "ai.lzy.worker.Worker",
            "lzy-service.gc.enabled", false
        );

        var environments = LzyConfig.Environments.builder()
            .addIamEnvironment(ai.lzy.iam.BeanFactory.TEST_ENV_NAME)
            .addAllocatorEnvironment(ai.lzy.allocator.BeanFactory.TEST_ENV_NAME)
            .addChannelManagerEnvironment(ai.lzy.channelmanager.BeanFactory.TEST_ENV_NAME)
            .addGraphExecutorEnvironment(ai.lzy.graph.BeanFactory.TEST_ENV_NAME)
            .addGraphExecutorEnvironment(ai.lzy.graph.BeanFactory.GRAPH_EXEC_DECORATOR_ENV_NAME)
            .addLzyServiceEnvironment(ai.lzy.service.BeanFactory.TEST_ENV_NAME)
            .build();

        var ports = LzyConfig.Ports.findFree();

        var database = LzyConfig.Database.builder()
            .setIamDbUrl(prepareDbUrl(iamDb.getConnectionInfo()))
            .setAllocatorDbUrl(prepareDbUrl(allocatorDb.getConnectionInfo()))
            .setChannelManagerDbUrl(prepareDbUrl(channelManagerDb.getConnectionInfo()))
            .setGraphExecutorDbUrl(prepareDbUrl(graphExecutorDb.getConnectionInfo()))
            .setStorageServiceDbUrl(prepareDbUrl(storageDb.getConnectionInfo()))
            .setLzyServiceDbUrl(prepareDbUrl(lzyServiceDb.getConnectionInfo()))
            .build();

        var contextEnvs = new String[] {
            IamContextImpl.ENV_NAME,
            AllocatorContextImpl.ENV_NAME,
            ChannelManagerContextImpl.ENV_NAME,
            GraphExecutorContextImpl.ENV_NAME,
            StorageContextImpl.ENV_NAME,
            LzyServiceContextImpl.ENV_NAME
        };
        lzy.setUp(configs, configOverrides, environments, ports, database, contextEnvs);

        var internalUserCredentials = lzy.micronautContext().getBean(IamContextImpl.class).clientConfig()
            .createRenewableToken();

        iamServiceGrpcChannel = newGrpcChannel(HostAndPort.fromParts("localhost", ports.getIamPort()),
            LzySubjectServiceGrpc.SERVICE_NAME, LzyAccessBindingServiceGrpc.SERVICE_NAME);
        iamClient = new SubjectServiceGrpcClient(CLIENT_NAME, iamServiceGrpcChannel, internalUserCredentials::get);

        lzyServiceGrpcChannel = newGrpcChannel(HostAndPort.fromParts("localhost", ports.getLzyServicePort()),
            LzyWorkflowServiceGrpc.SERVICE_NAME, LzyWorkflowPrivateServiceGrpc.SERVICE_NAME);

        lzyClient = newBlockingClient(LzyWorkflowServiceGrpc.newBlockingStub(lzyServiceGrpcChannel), CLIENT_NAME, null);
        unauthLzyPrivateClient = LzyWorkflowPrivateServiceGrpc.newBlockingStub(lzyServiceGrpcChannel);
        lzyPrivateClient = newBlockingClient(unauthLzyPrivateClient, CLIENT_NAME,
            () -> internalUserCredentials.get().token());
    }

    @After
    public final void tearDown() throws Exception {
        lzyServiceGrpcChannel.shutdown();
        try {
            lzyServiceGrpcChannel.awaitTermination(5, TimeUnit.SECONDS);
        } finally {
            lzyServiceGrpcChannel.shutdownNow();
            iamServiceGrpcChannel.shutdown();
            try {
                iamServiceGrpcChannel.awaitTermination(5, TimeUnit.SECONDS);
            } finally {
                iamServiceGrpcChannel.shutdownNow();
                lzy.tearDown();
            }
        }
    }

    public static String startWorkflow(LzyWorkflowServiceBlockingStub authLzyGrpcClient, String workflowName) {
        var fromResponse = RequestIdInterceptor.fromResponse();

        var execId = withIdempotencyKey(authLzyGrpcClient, "start_wf")
            .withInterceptors(fromResponse)
            .startWorkflow(
                LWFS.StartWorkflowRequest.newBuilder()
                    .setWorkflowName(workflowName)
                    .setSnapshotStorage(LMST.StorageConfig.getDefaultInstance())
                    .build())
            .getExecutionId();

        System.out.printf("StartWorkflow '%s' reqid: %s%n", workflowName, fromResponse.rid());
        return execId;
    }

    public static void finishWorkflow(LzyWorkflowServiceBlockingStub authLzyGrpcClient,
                                      String workflowName, String executionId)
    {
        var fromResponse = RequestIdInterceptor.fromResponse();

        // noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(authLzyGrpcClient, "finish_wf")
            .withInterceptors(fromResponse)
            .finishWorkflow(
                LWFS.FinishWorkflowRequest.newBuilder()
                    .setWorkflowName(workflowName)
                    .setExecutionId(executionId)
                    .setReason("no-matter")
                    .build());

        System.out.printf("FinishWorkflow '%s/%s' reqid: %s%n", workflowName, executionId, fromResponse.rid());
    }
}
