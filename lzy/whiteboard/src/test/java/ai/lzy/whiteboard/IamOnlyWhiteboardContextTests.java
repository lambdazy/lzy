package ai.lzy.whiteboard;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.test.IamContextImpl;
import ai.lzy.test.context.LzyInThread;
import ai.lzy.test.context.config.LzyConfig;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import ai.lzy.v1.whiteboard.LzyWhiteboardServiceGrpc.LzyWhiteboardServiceBlockingStub;
import ai.lzy.whiteboard.test.WhiteboardServiceContextImpl;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.zonky.test.db.postgres.embedded.ConnectionInfo;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.v1.whiteboard.LzyWhiteboardServiceGrpc.newBlockingStub;

public abstract class IamOnlyWhiteboardContextTests {
    private static final String CLIENT_NAME = "TestClient";

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule whiteboardBb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    public LzyInThread lzy = new LzyInThread();

    protected RenewableJwt internalUserCredentials;
    protected IamClientConfiguration iamClientConfig;
    private ManagedChannel iamServiceGrpcChannel;
    protected SubjectServiceGrpcClient iamClient;

    private ManagedChannel whiteboardGrpcChannel;
    protected LzyWhiteboardServiceBlockingStub whiteboardClient;

    public static String prepareDbUrl(ConnectionInfo ci) {
        return "jdbc:postgresql://localhost:%d/%s".formatted(ci.getPort(), ci.getDbName());
    }

    @Before
    public final void setUp() throws Exception {
        var configs = LzyConfig.Configs.builder()
            .setIamConfig("../iam/src/main/resources/application-test.yml")
            .setWhiteboardConfig("../whiteboard/src/main/resources/application-test.yml")
            .build();

        var environments = LzyConfig.Environments.builder()
            .addIamEnvironment(ai.lzy.iam.BeanFactory.TEST_ENV_NAME)
            .build();

        var ports = LzyConfig.Ports.findFree();

        var database = LzyConfig.Database.builder()
            .setIamDbUrl(prepareDbUrl(iamDb.getConnectionInfo()))
            .setWhiteboardDbUrl(prepareDbUrl(whiteboardBb.getConnectionInfo()))
            .build();

        lzy.setUp(configs, environments, ports, database, IamContextImpl.ENV_NAME,
            WhiteboardServiceContextImpl.ENV_NAME);

        iamClientConfig = lzy.micronautContext().getBean(IamContextImpl.class).clientConfig();
        internalUserCredentials = iamClientConfig.createRenewableToken();
        iamServiceGrpcChannel = newGrpcChannel(HostAndPort.fromParts("localhost", ports.getIamPort()),
            LzySubjectServiceGrpc.SERVICE_NAME, LzyAccessBindingServiceGrpc.SERVICE_NAME);
        iamClient = new SubjectServiceGrpcClient(CLIENT_NAME, iamServiceGrpcChannel, internalUserCredentials::get);

        whiteboardGrpcChannel = newGrpcChannel(HostAndPort.fromParts("localhost", ports.getWhiteboardPort()));
        whiteboardClient = newBlockingClient(newBlockingStub(whiteboardGrpcChannel), CLIENT_NAME, null);
    }

    @After
    public final void tearDown() throws Exception {
        whiteboardGrpcChannel.shutdown();
        try {
            whiteboardGrpcChannel.awaitTermination(5, TimeUnit.SECONDS);
        } finally {
            whiteboardGrpcChannel.shutdownNow();
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
