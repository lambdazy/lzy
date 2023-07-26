package ai.lzy.site.routes.context;

import ai.lzy.iam.test.IamContextImpl;
import ai.lzy.scheduler.test.SchedulerContextImpl;
import ai.lzy.site.ServiceConfig;
import ai.lzy.test.context.LzyInThread;
import ai.lzy.test.context.config.LzyConfig;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.embedded.ConnectionInfo;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

public abstract class IamAndSchedulerSiteContextTests extends AuthAwareTestContext {
    private static final String CLIENT_NAME = "TestClient";

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule schedulerDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    public LzyInThread lzy = new LzyInThread();

    @Before
    public final void setUp() throws InterruptedException {
        var configs = LzyConfig.Configs.builder()
            .setIamConfig("../iam/src/main/resources/application-test.yml")
            .setSchedulerConfig("../scheduler/src/main/resources/application-test.yml")
            .build();

        var environments = LzyConfig.Environments.builder()
            .addIamEnvironment(ai.lzy.iam.BeanFactory.TEST_ENV_NAME)
            .addSchedulerEnvironment("site-test")
            .build();

        var ports = LzyConfig.Ports.findFree();

        var database = LzyConfig.Database.builder()
            .setIamDbUrl(prepareDbUrl(iamDb.getConnectionInfo()))
            .setSchedulerDbUrl(prepareDbUrl(schedulerDb.getConnectionInfo()))
            .build();

        lzy.setUp(configs, environments, ports, database, IamContextImpl.ENV_NAME, SchedulerContextImpl.ENV_NAME);

        var config = lzy.micronautContext().getBean(ServiceConfig.class);
        config.getIam().setAddress("localhost:" + ports.getIamPort());
        config.setSchedulerAddress("localhost:" + ports.getSchedulerPort());

        setUpAuth();
    }

    @After
    public final void tearDown() {
        lzy.tearDown();
    }

    @Override
    protected ApplicationContext micronautContext() {
        return lzy.micronautContext();
    }

    public static String prepareDbUrl(ConnectionInfo ci) {
        return "jdbc:postgresql://localhost:%d/%s".formatted(ci.getPort(), ci.getDbName());
    }
}
