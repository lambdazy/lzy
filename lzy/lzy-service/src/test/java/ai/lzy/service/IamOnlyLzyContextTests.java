package ai.lzy.service;

import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.test.IamContextImpl;
import ai.lzy.service.test.LzyServiceContextImpl;
import ai.lzy.service.util.ClientVersionInterceptor;
import ai.lzy.test.context.LzyInThread;
import ai.lzy.test.context.config.LzyConfig;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
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

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public abstract class IamOnlyLzyContextTests {
    private static final String CLIENT_NAME = "TestClient";

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule lzyServiceDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    public LzyInThread lzy = new LzyInThread();

    private ManagedChannel iamServiceGrpcChannel;
    private ManagedChannel lzyServiceGrpcChannel;

    protected LzyWorkflowServiceBlockingStub lzyClient;
    protected LzyWorkflowPrivateServiceBlockingStub lzyPrivateClient;
    protected SubjectServiceGrpcClient iamClient;

    public static String prepareDbUrl(ConnectionInfo ci) {
        return "jdbc:postgresql://localhost:%d/%s".formatted(ci.getPort(), ci.getDbName());
    }

    @Before
    public final void setUp() throws Exception {
        ClientVersionInterceptor.DISABLE_VERSION_CHECK.set(true);

        var configs = LzyConfig.Configs.builder()
            .setIamConfig("../iam/src/main/resources/application-test.yml")
            .setLzyServiceConfig("../lzy-service/src/main/resources/application-test.yml")
            .build();

        var environments = LzyConfig.Environments.builder()
            .addIamEnvironment(ai.lzy.iam.BeanFactory.TEST_ENV_NAME)
            .addLzyServiceEnvironment(ai.lzy.service.BeanFactory.TEST_ENV_NAME)
            .build();

        var ports = LzyConfig.Ports.findFree();

        var database = LzyConfig.Database.builder()
            .setIamDbUrl(prepareDbUrl(iamDb.getConnectionInfo()))
            .setLzyServiceDbUrl(prepareDbUrl(lzyServiceDb.getConnectionInfo()))
            .build();

        lzy.setUp(configs, Map.of("lzy-service.gc.enabled", false), environments, ports, database,
            IamContextImpl.ENV_NAME, LzyServiceContextImpl.ENV_NAME);

        var internalUserCredentials = lzy.micronautContext().getBean(IamContextImpl.class).clientConfig()
            .createRenewableToken();

        iamServiceGrpcChannel = newGrpcChannel(HostAndPort.fromParts("localhost", ports.getIamPort()),
            LzySubjectServiceGrpc.SERVICE_NAME, LzyAccessBindingServiceGrpc.SERVICE_NAME);
        iamClient = new SubjectServiceGrpcClient(CLIENT_NAME, iamServiceGrpcChannel, internalUserCredentials::get);

        lzyServiceGrpcChannel = newGrpcChannel(HostAndPort.fromParts("localhost", ports.getLzyServicePort()),
            LzyWorkflowServiceGrpc.SERVICE_NAME, LzyWorkflowPrivateServiceGrpc.SERVICE_NAME);

        lzyClient = newBlockingClient(LzyWorkflowServiceGrpc.newBlockingStub(lzyServiceGrpcChannel), CLIENT_NAME, null);
        lzyPrivateClient = newBlockingClient(
            LzyWorkflowPrivateServiceGrpc.newBlockingStub(lzyServiceGrpcChannel), CLIENT_NAME,
            () -> internalUserCredentials.get().token()
        );
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
}
