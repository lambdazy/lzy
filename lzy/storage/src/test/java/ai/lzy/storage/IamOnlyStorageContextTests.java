package ai.lzy.storage;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.test.IamContextImpl;
import ai.lzy.storage.test.StorageContextImpl;
import ai.lzy.test.context.LzyInThread;
import ai.lzy.test.context.config.LzyConfig;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import ai.lzy.v1.storage.LzyStorageServiceGrpc.LzyStorageServiceBlockingStub;
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

public abstract class IamOnlyStorageContextTests {
    private static final String CLIENT_NAME = "TestClient";

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule storageDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    public LzyInThread lzy = new LzyInThread();

    protected RenewableJwt internalUserCredentials;
    protected IamClientConfiguration iamClientConfig;
    private ManagedChannel iamServiceGrpcChannel;
    protected SubjectServiceGrpcClient iamClient;

    private ManagedChannel storageGrpcChannel;
    protected LzyStorageServiceBlockingStub storageClient;

    public static String prepareDbUrl(ConnectionInfo ci) {
        return "jdbc:postgresql://localhost:%d/%s".formatted(ci.getPort(), ci.getDbName());
    }

    @Before
    public final void setUp() throws Exception {
        var configs = LzyConfig.Configs.builder()
            .setIamConfig("../iam/src/main/resources/application-test.yml")
            .setStorageConfig("../storage/src/main/resources/application-test.yml")
            .build();

        var environments = LzyConfig.Environments.builder()
            .addIamEnvironment(ai.lzy.iam.BeanFactory.TEST_ENV_NAME)
            .build();

        var ports = LzyConfig.Ports.findFree();

        var database = LzyConfig.Database.builder()
            .setIamDbUrl(prepareDbUrl(iamDb.getConnectionInfo()))
            .setStorageServiceDbUrl(prepareDbUrl(storageDb.getConnectionInfo()))
            .build();

        lzy.setUp(configs, environments, ports, database, IamContextImpl.ENV_NAME, StorageContextImpl.ENV_NAME);

        iamClientConfig = lzy.micronautContext().getBean(IamContextImpl.class).clientConfig();
        internalUserCredentials = iamClientConfig.createRenewableToken();
        iamServiceGrpcChannel = newGrpcChannel(HostAndPort.fromParts("localhost", ports.getIamPort()),
            LzySubjectServiceGrpc.SERVICE_NAME, LzyAccessBindingServiceGrpc.SERVICE_NAME);
        iamClient = new SubjectServiceGrpcClient(CLIENT_NAME, iamServiceGrpcChannel, internalUserCredentials::get);

        storageGrpcChannel = newGrpcChannel(HostAndPort.fromParts("localhost", ports.getStoragePort()),
            LzyStorageServiceGrpc.SERVICE_NAME);
        storageClient = newBlockingClient(LzyStorageServiceGrpc.newBlockingStub(storageGrpcChannel), CLIENT_NAME, null);
    }

    @After
    public final void tearDown() throws Exception {
        storageGrpcChannel.shutdown();
        try {
            storageGrpcChannel.awaitTermination(5, TimeUnit.SECONDS);
        } finally {
            storageGrpcChannel.shutdownNow();
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
