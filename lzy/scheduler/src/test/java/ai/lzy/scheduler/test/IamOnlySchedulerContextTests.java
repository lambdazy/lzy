package ai.lzy.scheduler.test;

import ai.lzy.iam.test.IamContextImpl;
import ai.lzy.scheduler.SchedulerApi;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.test.SchedulerContextImpl;
import ai.lzy.test.context.LzyInThread;
import ai.lzy.test.context.config.LzyConfig;
import ai.lzy.v1.scheduler.SchedulerGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.embedded.ConnectionInfo;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.util.concurrent.TimeUnit;

import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class IamOnlySchedulerContextTests {
    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule schedulerDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    protected ApplicationContext context;
    protected SchedulerGrpc.SchedulerBlockingStub stub;
    protected ManagedChannel chan;

    public LzyInThread lzy = new LzyInThread();

    @Before
    public final void setUp() throws InterruptedException {
        var configs = LzyConfig.Configs.builder()
            .setIamConfig("../iam/src/main/resources/application-test.yml")
            .setSchedulerConfig("../scheduler/src/main/resources/application-test.yml")
            .build();

        var environments = LzyConfig.Environments.builder()
            .addIamEnvironment(ai.lzy.iam.BeanFactory.TEST_ENV_NAME)
            .build();

        var ports = LzyConfig.Ports.findFree();

        var database = LzyConfig.Database.builder()
            .setIamDbUrl(prepareDbUrl(iamDb.getConnectionInfo()))
            .setSchedulerDbUrl(prepareDbUrl(schedulerDb.getConnectionInfo()))
            .build();

        lzy.setUp(configs, preparePostgresConfig("jobs", schedulerDb.getConnectionInfo()), environments, ports,
            database, SchedulerContextImpl.ENV_NAME, IamContextImpl.ENV_NAME);

        var credentials = lzy.micronautContext().getBean(IamContextImpl.class).clientConfig().createRenewableToken();

        chan = newGrpcChannel("localhost", ports.getSchedulerPort(), SchedulerGrpc.SERVICE_NAME);
        stub = newBlockingClient(SchedulerGrpc.newBlockingStub(chan), "Test", () -> credentials.get().token());

        Configurator.setAllLevels("ai.lzy.scheduler", Level.ALL);
    }

    @After
    public final void tearDown() {
        chan.shutdown();
        try {
            chan.awaitTermination(60, TimeUnit.SECONDS);
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
