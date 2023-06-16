package ai.lzy.graph.test;

import java.util.List;

import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;

import ai.lzy.graph.GraphExecutorApi;
import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.v1.longrunning.LongRunning;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.Assert;
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
        GraphExecutorApi2.GraphExecuteRequest request = GraphExecutorApi2.GraphExecuteRequest.newBuilder()
            .setExecutionId("1")
            .setWorkflowName("workflow1")
            .setUserId("2")
            .addAllTasks(List.of(
                GraphExecutorApi2.GraphExecuteRequest.TaskDesc.newBuilder()
                    .setId("task-1")
                    .build()
            ))
            .build();
        final LongRunning.Operation[] op = new LongRunning.Operation[1];
        api.execute(request, new StreamObserver<>() {
            @Override
            public void onNext(LongRunning.Operation operation) {
                op[0] = operation;
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        });
        Assert.assertNotNull(op[0]);
    }
}
