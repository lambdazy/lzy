package ai.lzy.site.routes.context;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.test.IamContextImpl;
import ai.lzy.site.ServiceConfig;
import ai.lzy.test.context.LzyInThread;
import ai.lzy.test.context.config.LzyConfig;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.embedded.ConnectionInfo;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public abstract class IamOnlySiteContextTests extends AuthAwareTestContext {
    private static final String CLIENT_NAME = "TestClient";

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    public LzyInThread lzy = new LzyInThread();

    protected RenewableJwt internalUserCredentials;
    protected IamClientConfiguration iamClientConfig;
    protected ManagedChannel iamServiceGrpcChannel;
    protected SubjectServiceGrpcClient iamClient;

    @Before
    public final void setUpIam() throws InterruptedException {
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

        var config = lzy.micronautContext().getBean(ServiceConfig.class);
        config.getIam().setAddress("localhost:" + ports.getIamPort());

        iamClientConfig = lzy.micronautContext().getBean(IamContextImpl.class).clientConfig();
        internalUserCredentials = iamClientConfig.createRenewableToken();
        iamServiceGrpcChannel = newGrpcChannel(HostAndPort.fromParts("localhost", ports.getIamPort()),
            LzySubjectServiceGrpc.SERVICE_NAME, LzyAccessBindingServiceGrpc.SERVICE_NAME);
        iamClient = new SubjectServiceGrpcClient(CLIENT_NAME, iamServiceGrpcChannel, internalUserCredentials::get);

        setUpAuth();
    }

    @After
    public final void tearDownIam() throws InterruptedException {
        iamServiceGrpcChannel.shutdown();
        try {
            iamServiceGrpcChannel.awaitTermination(5, TimeUnit.SECONDS);
        } finally {
            iamServiceGrpcChannel.shutdownNow();
            lzy.tearDown();
        }
    }

    @Override
    protected ApplicationContext micronautContext() {
        return lzy.micronautContext();
    }

    public static String prepareDbUrl(ConnectionInfo ci) {
        return "jdbc:postgresql://localhost:%d/%s".formatted(ci.getPort(), ci.getDbName());
    }
}
