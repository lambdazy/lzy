package ai.lzy.graph.test;

import ai.lzy.graph.GraphExecutorApi;
import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.v1.longrunning.LongRunning;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import io.prometheus.client.CollectorRegistry;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.List;

import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;

public class GraphExecutorTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private GraphExecutorApi api;

    @Before
    public void setUp() {
        CollectorRegistry.defaultRegistry.clear();
        context = ApplicationContext.run(preparePostgresConfig("graph-executor-2", db.getConnectionInfo()), "test-mock");
        api = context.getBean(GraphExecutorApi.class);
    }

    @After
    public void after() {
        context.stop();
    }

    @Test
    public void simpleTest() throws Exception {
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
        LongRunning.Operation op = getResult(request);
        Assert.assertNotNull(op);
    }

    @Test
    public void errorOnValidationTest() {
        GraphExecutorApi2.GraphExecuteRequest request = GraphExecutorApi2.GraphExecuteRequest.newBuilder()
            .setExecutionId("1")
            .setWorkflowName("workflow1")
            .setUserId("2")
            .build();
        Assert.assertThrows(Exception.class, () -> getResult(request));
    }

    public LongRunning.Operation getResult(GraphExecutorApi2.GraphExecuteRequest request) throws Exception {
        final LongRunning.Operation[] op = new LongRunning.Operation[1];
        final Throwable[] ex = new Throwable[1];
        api.execute(request, new StreamObserver<>() {
            @Override
            public void onNext(LongRunning.Operation operation) {
                op[0] = operation;
            }

            @Override
            public void onError(Throwable throwable) {
                ex[0] = throwable;
            }

            @Override
            public void onCompleted() {

            }
        });

        if (ex[0] != null) {
            throw new Exception(ex[0]);
        }

        return op[0];
    }
}
