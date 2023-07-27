package ai.lzy.channelmanager;

import ai.lzy.channelmanager.test.ChannelManagerContextImpl;
import ai.lzy.iam.test.IamContextImpl;
import ai.lzy.test.context.LzyInThread;
import ai.lzy.test.context.config.LzyConfig;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.embedded.ConnectionInfo;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class IamOnlyChannelManagerContextTests {
    @ClassRule
    public static PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @ClassRule
    public static PreparedDbRule channelManagerDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    public static LzyInThread lzy = new LzyInThread();

    protected static ApplicationContext context;
    protected static ManagedChannel channelManagerGrpcChannel;
    protected static LzyChannelManagerPrivateBlockingStub privateClient;

    @BeforeClass
    public static void setUp() throws InterruptedException {
        var configs = LzyConfig.Configs.builder()
            .setIamConfig("../iam/src/main/resources/application-test.yml")
            .setChannelManagerConfig("../channel-manager/src/main/resources/application-test.yml")
            .build();

        var environments = LzyConfig.Environments.builder()
            .addIamEnvironment(ai.lzy.iam.BeanFactory.TEST_ENV_NAME)
            .addChannelManagerEnvironment(ai.lzy.channelmanager.BeanFactory.TEST_ENV_NAME)
            .build();

        var ports = LzyConfig.Ports.findFree();

        var database = LzyConfig.Database.builder()
            .setIamDbUrl(prepareDbUrl(iamDb.getConnectionInfo()))
            .setChannelManagerDbUrl(prepareDbUrl(channelManagerDb.getConnectionInfo()))
            .build();

        lzy.setUp(configs, environments, ports, database, ChannelManagerContextImpl.ENV_NAME, IamContextImpl.ENV_NAME);
        context = lzy.micronautContext().getBean(ChannelManagerContextImpl.class).getMicronautContext();

        channelManagerGrpcChannel = newGrpcChannel(HostAndPort.fromParts("localhost", ports.getChannelManagerPort()),
            LzyChannelManagerPrivateGrpc.SERVICE_NAME);
        var internalUserCredentials = lzy.micronautContext().getBean(IamContextImpl.class).clientConfig()
            .createRenewableToken();
        privateClient = newBlockingClient(LzyChannelManagerPrivateGrpc.newBlockingStub(channelManagerGrpcChannel),
            "AuthPrivateTest", () -> internalUserCredentials.get().token());
    }

    @AfterClass
    public static void tearDown() {
        channelManagerGrpcChannel.shutdown();
        try {
            channelManagerGrpcChannel.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            //ignored
        } finally {
            lzy.tearDown();
        }
    }

    public static String prepareDbUrl(ConnectionInfo ci) {
        return "jdbc:postgresql://localhost:%d/%s".formatted(ci.getPort(), ci.getDbName());
    }
}
