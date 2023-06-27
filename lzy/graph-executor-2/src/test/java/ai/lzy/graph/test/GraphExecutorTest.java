package ai.lzy.graph.test;

import java.util.List;

import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;

import ai.lzy.graph.GraphExecutorApi;
import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.v1.longrunning.LongRunning;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit5.EmbeddedPostgresExtension;
import io.zonky.test.db.postgres.junit5.PreparedDbExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;


public class GraphExecutorTest {
    @RegisterExtension
    public static PreparedDbExtension db = EmbeddedPostgresExtension.preparedDatabase(ds -> {});

    private static GraphExecutorApi api;

    @BeforeAll
    public static void setUp() {
        ApplicationContext context =
            ApplicationContext.run(preparePostgresConfig("graph-executor-2", db.getConnectionInfo()), "test-mock");
        api = context.getBean(GraphExecutorApi.class);
    }

    @Test
    @Timeout(5)
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
        Assertions.assertNotNull(op);
    }

    @Test
    @Timeout(5)
    public void errorOnValidationTest() {
        GraphExecutorApi2.GraphExecuteRequest request = GraphExecutorApi2.GraphExecuteRequest.newBuilder()
            .setExecutionId("1")
            .setWorkflowName("workflow1")
            .setUserId("2")
            .build();
        Assertions.assertThrows(Exception.class, () -> getResult(request));
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
