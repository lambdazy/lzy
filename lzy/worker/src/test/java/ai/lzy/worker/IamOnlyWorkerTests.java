package ai.lzy.worker;

import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.test.IamContextImpl;
import ai.lzy.test.context.LzyInThread;
import ai.lzy.test.context.config.LzyConfig;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.zonky.test.db.postgres.embedded.ConnectionInfo;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public abstract class IamOnlyWorkerTests {
    private static final String CLIENT_NAME = "TestClient";

    @ClassRule
    public static PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    public static LzyInThread lzy = new LzyInThread();

    protected static int iamPort;
    private static ManagedChannel iamServiceGrpcChannel;
    protected static RenewableJwt internalUserCredentials;
    protected static SubjectServiceGrpcClient subjectIamClient;
    protected static AccessBindingServiceGrpcClient abIamClient;

    public static String prepareDbUrl(ConnectionInfo ci) {
        return "jdbc:postgresql://localhost:%d/%s".formatted(ci.getPort(), ci.getDbName());
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        var configs = LzyConfig.Configs.builder()
            .setIamConfig("../iam/src/main/resources/application-test.yml")
            .build();

        var environments = LzyConfig.Environments.builder()
            .addIamEnvironment(ai.lzy.iam.BeanFactory.TEST_ENV_NAME)
            .build();

        var ports = LzyConfig.Ports.findFree();

        var database = LzyConfig.Database.builder()
            .setIamDbUrl(prepareDbUrl(iamDb.getConnectionInfo()))
            .build();

        lzy.setUp(configs, environments, ports, database, IamContextImpl.ENV_NAME);

        iamPort = ports.getIamPort();
        internalUserCredentials = lzy.micronautContext().getBean(IamContextImpl.class).clientConfig()
            .createRenewableToken();

        iamServiceGrpcChannel = newGrpcChannel(HostAndPort.fromParts("localhost", ports.getIamPort()),
            LzySubjectServiceGrpc.SERVICE_NAME, LzyAccessBindingServiceGrpc.SERVICE_NAME);
        subjectIamClient =
            new SubjectServiceGrpcClient(CLIENT_NAME, iamServiceGrpcChannel, internalUserCredentials::get);
        abIamClient =
            new AccessBindingServiceGrpcClient(CLIENT_NAME, iamServiceGrpcChannel, internalUserCredentials::get);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        iamServiceGrpcChannel.shutdown();
        try {
            iamServiceGrpcChannel.awaitTermination(5, TimeUnit.SECONDS);
        } finally {
            iamServiceGrpcChannel.shutdownNow();
            lzy.tearDown();
        }
    }
}
