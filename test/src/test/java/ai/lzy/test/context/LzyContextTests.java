package ai.lzy.test.context;

import ai.lzy.allocator.test.AllocatorContextImpl;
import ai.lzy.channelmanager.test.ChannelManagerContextImpl;
import ai.lzy.graph.test.GraphExecutorContextImpl;
import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.test.IamContextImpl;
import ai.lzy.scheduler.test.SchedulerContextImpl;
import ai.lzy.service.test.LzyServiceContextImpl;
import ai.lzy.service.util.ClientVersionInterceptor;
import ai.lzy.storage.test.StorageContextImpl;
import ai.lzy.test.context.config.LzyConfig;
import ai.lzy.test.Utils;
import ai.lzy.test.KafkaContext;
import ai.lzy.util.auth.credentials.*;
import ai.lzy.util.grpc.RequestIdInterceptor;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.AllocatorPrivateGrpc;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc.LzyWorkflowPrivateServiceBlockingStub;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import ai.lzy.whiteboard.test.WhiteboardServiceContextImpl;
import ai.lzy.worker.Worker;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.embedded.ConnectionInfo;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.*;

public abstract class LzyContextTests implements AllocatorBeans, GraphExecutorBeans, LzyServiceBeans {
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
    public PreparedDbRule schedulerDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule storageDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule whiteboardDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule lzyServiceDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    protected LzyConfig.Ports ports;
    public LzyInThread lzy = new LzyInThread();

    protected User testUser;
    private RenewableJwt internalUserCredential;
    private ManagedChannel channelsGrpcChannel;
    private ManagedChannel iamGrpcChannel;
    private ManagedChannel graphsGrpcChannel;
    private ManagedChannel lzyServiceGrpcChannel;
    private ManagedChannel allocatorGrpcChannel;

    protected LzyWorkflowServiceBlockingStub lzyGrpcClient;
    protected LzyWorkflowPrivateServiceBlockingStub privateLzyGrpcClient;
    protected LzyChannelManagerPrivateBlockingStub privateChannelsGrpcClient;
    protected GraphExecutorBlockingStub graphsGrpcClient;
    protected AllocatorBlockingStub allocatorGrpcClient;
    protected SubjectServiceGrpcClient subjectServiceGrpcClient;

    protected KafkaContext kafka = new KafkaContext();

    @Override
    public ApplicationContext allocatorContext() {
        return lzy.micronautContext().getBean(AllocatorContextImpl.class).getMicronautContext();
    }

    @Override
    public ApplicationContext lzyServiceContext() {
        return lzy.micronautContext().getBean(LzyServiceContextImpl.class).getMicronautContext();
    }

    @Override
    public ApplicationContext graphExecutorContext() {
        return lzy.micronautContext().getBean(GraphExecutorContextImpl.class).getMicronautContext();
    }

    @Before
    public final void setUp() throws Exception {
        ClientVersionInterceptor.DISABLE_VERSION_CHECK.set(true);
        Worker.selectRandomValues(true);

        setUpLzyContext();
        setUpClients();
    }

    private void setUpLzyContext() throws InterruptedException {
        var configs = LzyConfig.Configs.builder()
            .setIamConfig("../lzy/iam/src/main/resources/application-test.yml")
            .setAllocatorConfig("../lzy/allocator/src/main/resources/application-test.yml")
            .setChannelManagerConfig("../lzy/channel-manager/src/main/resources/application-test.yml")
            .setGraphExecutorConfig("../lzy/graph-executor/src/main/resources/application-test.yml")
            .setSchedulerConfig("../lzy/scheduler/src/main/resources/application-test.yml")
            .setStorageConfig("../lzy/storage/src/main/resources/application-test.yml")
            .setWhiteboardConfig("../lzy/whiteboard/src/main/resources/application-test.yml")
            .setLzyServiceConfig("../lzy/lzy-service/src/main/resources/application-test.yml")
            .build();

        var environments = LzyConfig.Environments.builder()
            .addIamEnvironment(ai.lzy.iam.BeanFactory.TEST_ENV_NAME)
            .addAllocatorEnvironment(ai.lzy.allocator.BeanFactory.TEST_ENV_NAME)
            .addChannelManagerEnvironment(ai.lzy.channelmanager.BeanFactory.TEST_ENV_NAME)
            .addGraphExecutorEnvironment(ai.lzy.graph.BeanFactory.GRAPH_EXEC_DECORATOR_ENV_NAME)
            .addLzyServiceEnvironment(ai.lzy.service.BeanFactory.TEST_ENV_NAME)
            .build();

        ports = LzyConfig.Ports.findFree();

        var database = LzyConfig.Database.builder()
            .setIamDbUrl(prepareDbUrl(iamDb.getConnectionInfo()))
            .setAllocatorDbUrl(prepareDbUrl(allocatorDb.getConnectionInfo()))
            .setChannelManagerDbUrl(prepareDbUrl(channelManagerDb.getConnectionInfo()))
            .setGraphExecutorDbUrl(prepareDbUrl(graphExecutorDb.getConnectionInfo()))
            .setSchedulerDbUrl(prepareDbUrl(schedulerDb.getConnectionInfo()))
            .setStorageServiceDbUrl(prepareDbUrl(storageDb.getConnectionInfo()))
            .setWhiteboardDbUrl(prepareDbUrl(whiteboardDb.getConnectionInfo()))
            .setLzyServiceDbUrl(prepareDbUrl(lzyServiceDb.getConnectionInfo()))
            .build();

        var contextEnvs = new String[] {
            IamContextImpl.ENV_NAME,
            AllocatorContextImpl.ENV_NAME,
            ChannelManagerContextImpl.ENV_NAME,
            GraphExecutorContextImpl.ENV_NAME,
            SchedulerContextImpl.ENV_NAME,
            StorageContextImpl.ENV_NAME,
            WhiteboardServiceContextImpl.ENV_NAME,
            LzyServiceContextImpl.ENV_NAME
        };

        Utils.createFolder(Path.of("/tmp/resources"));
        Utils.createFolder(Path.of("/tmp/local_modules"));

        var allocatorConfigOverrides = Map.<String, Object>of(
            "allocator.instance-id", "xxx",
            "allocator.nfs-client-image", "xxx",
            "allocator.kuber-allocator.enabled", false,
            "allocator.thread-allocator.enabled", true,
            "allocator.thread-allocator.vm-jar-file", "../lzy/worker/target/worker.jar",
            "allocator.thread-allocator.vm-class-name", "ai.lzy.worker.Worker"
        );

        var channelManagerConfigOverrides = Map.<String, Object>of(
            "channel-manager.lock-buckets-count", 256,
            "channel-manager.executor-threads-count", 10,
            "channel-manager.connections.cache-concurrency-level", 10,
            "channel-manager.connections.cache-ttl", "20s"
        );

        String userDefaultImage = System.getProperty("scheduler.userTestImage", "lzydock/user-test:master");
        var graphExecConfigOverrides = Map.<String, Object>of(
            "graph-executor.executors-count", 1,
            "scheduler.kafka.bootstrap-servers", kafka.getBootstrapServers(),
            "scheduler.max-workers-per-workflow", 2,
            "scheduler.default-provisioning-limit", 2,
            "scheduler.user-default-image", userDefaultImage
        );

        var lzyServiceConfigOverrides = Map.<String, Object>of(
            "lzy-service.kafka.bootstrap-servers", kafka.getBootstrapServers(),
            "lzy-service.allocator-vm-cache-timeout", "2s",
            "lzy-service.gc.enabled", false,
            "lzy-service.gc.gc-period", "1s"
        );

        var configOverrides = new HashMap<String, Object>() {
            {
                putAll(allocatorConfigOverrides);
                putAll(channelManagerConfigOverrides);
                putAll(graphExecConfigOverrides);
                putAll(lzyServiceConfigOverrides);
            }
        };

        lzy.setUp(configs, configOverrides, environments, ports, database, contextEnvs);
    }

    private void setUpClients() throws Exception {
        internalUserCredential = lzy.micronautContext().getBean(IamContextImpl.class).clientConfig()
            .createRenewableToken();

        channelsGrpcChannel = newGrpcChannel("localhost", ports.getChannelManagerPort(),
            LzyChannelManagerGrpc.SERVICE_NAME, LzyChannelManagerPrivateGrpc.SERVICE_NAME);
        lzyServiceGrpcChannel = newGrpcChannel("localhost", ports.getLzyServicePort(),
            LzyWorkflowServiceGrpc.SERVICE_NAME, LzyWorkflowPrivateServiceGrpc.SERVICE_NAME);
        iamGrpcChannel = newGrpcChannel("localhost", ports.getIamPort(), LzySubjectServiceGrpc.SERVICE_NAME,
            LzyAccessBindingServiceGrpc.SERVICE_NAME, LzyAuthenticateServiceGrpc.SERVICE_NAME);
        graphsGrpcChannel = newGrpcChannel("localhost", ports.getGraphExecutorPort(), GraphExecutorGrpc.SERVICE_NAME);
        allocatorGrpcChannel = newGrpcChannel("localhost", ports.getAllocatorPort(), AllocatorGrpc.SERVICE_NAME,
            AllocatorPrivateGrpc.SERVICE_NAME);

        testUser = IamClient.createUser("test-user-1", iamGrpcChannel, internalUserCredential);

        lzyGrpcClient = newBlockingClient(LzyWorkflowServiceGrpc.newBlockingStub(lzyServiceGrpcChannel), CLIENT_NAME,
            () -> testUser.credentials().token());
        privateLzyGrpcClient = newBlockingClient(LzyWorkflowPrivateServiceGrpc.newBlockingStub(lzyServiceGrpcChannel),
            CLIENT_NAME, () -> internalUserCredential.get().token());
        privateChannelsGrpcClient = newBlockingClient(LzyChannelManagerPrivateGrpc.newBlockingStub(channelsGrpcChannel),
            CLIENT_NAME, () -> internalUserCredential.get().token());
        graphsGrpcClient = newBlockingClient(GraphExecutorGrpc.newBlockingStub(graphsGrpcChannel), CLIENT_NAME,
            () -> internalUserCredential.get().token());
        allocatorGrpcClient = newBlockingClient(AllocatorGrpc.newBlockingStub(allocatorGrpcChannel), CLIENT_NAME,
            () -> internalUserCredential.get().token());

        subjectServiceGrpcClient =  new SubjectServiceGrpcClient(CLIENT_NAME, iamGrpcChannel,
            () -> internalUserCredential.get());
    }

    @After
    public final void tearDown() throws InterruptedException {
        try {
            tearDownClients();
        } finally {
            lzy.tearDown();
            kafka.close();
        }
    }

    private void tearDownClients() throws InterruptedException {
        lzyServiceGrpcChannel.shutdown();
        try {
            lzyServiceGrpcChannel.awaitTermination(5, TimeUnit.SECONDS);
        } finally {
            lzyServiceGrpcChannel.shutdownNow();
            iamGrpcChannel.shutdown();
            try {
                iamGrpcChannel.awaitTermination(5, TimeUnit.SECONDS);
            } finally {
                iamGrpcChannel.shutdownNow();
                channelsGrpcChannel.shutdown();
                try {
                    channelsGrpcChannel.awaitTermination(5, TimeUnit.SECONDS);
                } finally {
                    channelsGrpcChannel.shutdownNow();
                }
            }
        }
    }

    public static String startWorkflow(LzyWorkflowServiceBlockingStub authLzyGrpcClient,
                                       String workflowName)
    {
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

    public static String prepareDbUrl(ConnectionInfo ci) {
        return "jdbc:postgresql://localhost:%d/%s".formatted(ci.getPort(), ci.getDbName());
    }

    public record User(String id, JwtCredentials credentials) {}

    public interface IamClient {
        static User createUser(String name, ManagedChannel iamChannel, RenewableJwt internalUserCredentials)
            throws Exception
        {
            var login = "github-" + name;
            var creds = generateCredentials(login);
            var subjectCreds = new SubjectCredentials("main", creds.publicKey(), CredentialsType.PUBLIC_KEY);

            var subj = new SubjectServiceGrpcClient(CLIENT_NAME, iamChannel, internalUserCredentials::get)
                .createSubject(AuthProvider.GITHUB, login, SubjectType.USER, subjectCreds);

            return new User(subj.id(), creds.credentials());
        }

        static void addWorkflowAccess(User user, String workflowName, ManagedChannel iamChannel,
                                      RenewableJwt internalUserCredentials)
        {
            var subj = new SubjectServiceGrpcClient(CLIENT_NAME, iamChannel, internalUserCredentials::get)
                .getSubject(user.id);
            var wf = new Workflow(user.id + "/" + workflowName);
            var abList = List.of(new AccessBinding(Role.LZY_WORKFLOW_OWNER, subj));

            new AccessBindingServiceGrpcClient(CLIENT_NAME, iamChannel, internalUserCredentials::get)
                .setAccessBindings(wf, abList);
        }

        private static IamClient.GeneratedCredentials generateCredentials(String login)
            throws IOException, InterruptedException, NoSuchAlgorithmException, InvalidKeySpecException
        {
            final var keys = RsaUtils.generateRsaKeys();
            var from = Date.from(Instant.now());
            var till = JwtUtils.afterDays(7);
            var credentials = new JwtCredentials(JwtUtils.buildJWT(login, "GITHUB", from, till,
                CredentialsUtils.readPrivateKey(keys.privateKey())));

            final var publicKey = keys.publicKey();

            return new IamClient.GeneratedCredentials(publicKey, credentials);
        }

        record GeneratedCredentials(String publicKey, JwtCredentials credentials) {}
    }
}
