package ru.yandex.cloud.ml.platform.lzy.graph.test;

import ru.yandex.cloud.ml.platform.lzy.graph.exec.BfsGraphProcessor;
import ru.yandex.cloud.ml.platform.lzy.graph.exec.GraphProcessor;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.graph.exec.GraphExecutor;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.BfsGraphBuilder;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.GraphBuilder;
import ru.yandex.cloud.ml.platform.lzy.graph.config.ServiceConfig;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;
import ru.yandex.cloud.ml.platform.lzy.graph.test.mocks.GraphDaoMock;
import ru.yandex.cloud.ml.platform.lzy.graph.test.mocks.SchedulerApiMock;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@MicronautTest
public class GraphExecutorTest {

    private SchedulerApiMock scheduler;
    private GraphDaoMock dao;
    private GraphExecutor executor;
    private final int TIMEOUT = 300;

    @Before
    public void setUp() {
        scheduler =  new SchedulerApiMock((a, b, sch) -> {
            sch.changeStatus(b.id(), Tasks.TaskProgress.newBuilder()
                .setTid(b.id())
                .setStatus(Tasks.TaskProgress.Status.QUEUE)
                .build()
            );
            return b.id();
        });

        dao = new GraphDaoMock();

        GraphBuilder builder = new BfsGraphBuilder(scheduler);

        GraphProcessor processor = new BfsGraphProcessor(scheduler, builder);

        ServiceConfig config = new ServiceConfig();
        config.setProcessingPeriodMillis(100);
        config.setThreadPoolSize(16);

        executor = new GraphExecutor(dao, processor, config);
    }

    @Test
    public void testSimple() throws InterruptedException {
        final GraphDescription graph = new BfsGraphBuilderTest.GraphDescriptionBuilder()
            .addVertexes("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
            .addEdge("1", "2")
            .addEdge("3", "2")
            .addEdge("3", "4")
            .addEdge("5", "6")
            .addEdge("7", "6")
            .addEdge("7", "8")
            .addEdge("9", "10")
            .addEdge("10", "8")
            .build();

        GraphExecutionState state = dao.create("", graph);
        executor.start();

        scheduler.waitForStatus("1", Tasks.TaskProgress.Status.QUEUE, TIMEOUT);

        var state2 = dao.get("", state.id());
        Assert.assertEquals(
            Set.of("1", "3", "5", "7", "9"),
            state2.currentExecutionGroup().stream().map(t -> t.description().id()).collect(Collectors.toSet())
        );


        scheduler.changeStatus("1", SchedulerApiMock.EXECUTING);
        scheduler.changeStatus("5", SchedulerApiMock.EXECUTING);
        scheduler.changeStatus("7", SchedulerApiMock.EXECUTING);
        scheduler.changeStatus("9", SchedulerApiMock.EXECUTING);

        scheduler.waitForStatus("6", Tasks.TaskProgress.Status.QUEUE, TIMEOUT);

        var state3 = dao.get("", state.id());
        Assert.assertEquals(
            Set.of("3", "6", "10"),
            state3.currentExecutionGroup().stream().map(t -> t.description().id()).collect(Collectors.toSet())
        );
        Assert.assertEquals(GraphExecutionState.Status.EXECUTING, state3.status());

        scheduler.changeStatus("10", SchedulerApiMock.EXECUTING);
        scheduler.changeStatus("6", SchedulerApiMock.EXECUTING);

        scheduler.waitForStatus("8", Tasks.TaskProgress.Status.QUEUE, TIMEOUT);
        var state4 = dao.get("", state.id());
        Assert.assertEquals(
                Set.of("3", "8"),
                state4.currentExecutionGroup().stream().map(t -> t.description().id()).collect(Collectors.toSet())
        );

        scheduler.changeStatus("8", SchedulerApiMock.EXECUTING);
        scheduler.changeStatus("3", SchedulerApiMock.EXECUTING);

        scheduler.waitForStatus("2", Tasks.TaskProgress.Status.QUEUE, TIMEOUT);
        var state5 = dao.get("", state.id());
        Assert.assertEquals(
                Set.of("2", "4"),
                state5.currentExecutionGroup().stream().map(t -> t.description().id()).collect(Collectors.toSet())
        );

        scheduler.changeStatus("2", SchedulerApiMock.EXECUTING);
        scheduler.changeStatus("4", SchedulerApiMock.EXECUTING);

        for (String tid: List.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")) {
            scheduler.changeStatus(tid, SchedulerApiMock.COMPLETED);
        }

        Thread.sleep(300);

        Assert.assertEquals(dao.get("", state.id()).status(), GraphExecutionState.Status.COMPLETED);

        executor.gracefulStop();
    }

    @Test
    public void testErrorInTask() throws InterruptedException {

        final GraphDescription graph = new BfsGraphBuilderTest.GraphDescriptionBuilder()
            .addVertexes("1", "2", "3")
            .addEdge("1", "2")
            .addEdge("1", "3")
            .build();

        GraphExecutionState state = dao.create("", graph);
        executor.start();

        scheduler.waitForStatus("1", Tasks.TaskProgress.Status.QUEUE, TIMEOUT);
        scheduler.changeStatus("1", SchedulerApiMock.EXECUTING);

        scheduler.waitForStatus("2", Tasks.TaskProgress.Status.QUEUE, TIMEOUT);
        scheduler.changeStatus("2", SchedulerApiMock.EXECUTING);
        scheduler.changeStatus("3", SchedulerApiMock.EXECUTING);

        scheduler.changeStatus("1", SchedulerApiMock.ERROR);

        scheduler.waitForStatus("2", Tasks.TaskProgress.Status.ERROR, TIMEOUT); // wait for kill from executor

        Assert.assertEquals(dao.get("", state.id()).status(), GraphExecutionState.Status.FAILED);
        executor.gracefulStop();

    }

    @Test
    public void testStop() throws InterruptedException {
        final GraphDescription graph = new BfsGraphBuilderTest.GraphDescriptionBuilder()
            .addVertexes("1", "2", "3")
            .addEdge("1", "2")
            .addEdge("1", "3")
            .build();

        GraphExecutionState state = dao.create("", graph);
        executor.start();

        scheduler.waitForStatus("1", Tasks.TaskProgress.Status.QUEUE, TIMEOUT);
        scheduler.changeStatus("1", SchedulerApiMock.EXECUTING);

        scheduler.waitForStatus("2", Tasks.TaskProgress.Status.QUEUE, TIMEOUT);
        scheduler.changeStatus("2", SchedulerApiMock.EXECUTING);
        scheduler.changeStatus("3", SchedulerApiMock.EXECUTING);

        dao.updateAtomic("", state.id(), s -> new GraphExecutionState(
            s.workflowId(),
            s.id(),
            s.description(),
            s.executions(),
            s.currentExecutionGroup(),
            GraphExecutionState.Status.SCHEDULED_TO_FAIL,
            "Stopped by test"
        ));

        scheduler.waitForStatus("1", Tasks.TaskProgress.Status.ERROR, TIMEOUT);
        scheduler.waitForStatus("2", Tasks.TaskProgress.Status.ERROR, TIMEOUT);
        scheduler.waitForStatus("3", Tasks.TaskProgress.Status.ERROR, TIMEOUT);

        Assert.assertEquals(dao.get("", state.id()).status(), GraphExecutionState.Status.FAILED);
    }
}
