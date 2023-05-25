package ai.lzy.graph.test;

import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;

import ai.lzy.graph.GraphExecutorApi;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.v1.longrunning.LongRunning;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class GraphExecutorTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private OperationDao operationDao;
    private GraphExecutorApi api;

    @Before
    public void setUp() {
        context = ApplicationContext.run(preparePostgresConfig("graph-executor-2", db.getConnectionInfo()), "test-mock");
        api = context.getBean(GraphExecutorApi.class);
        operationDao = context.getBean(OperationDao.class, Qualifiers.byName("GraphExecutorOperationDao"));
    }

    @Test
    public void test() {
        api.execute(null, new StreamObserver<>() {
            @Override
            public void onNext(LongRunning.Operation operation) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        });
    }
}
