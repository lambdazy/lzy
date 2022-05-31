package ru.yandex.cloud.ml.platform.lzy.graph_executor.test;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.Assert;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.GraphWatcher;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.algo.BfsGraphBuilder;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphExecutionState;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.test.mocks.GraphDaoMock;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.test.mocks.SchedulerApiMock;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@MicronautTest
public class GraphWatcherTest {

    @Test
    public void testSimple() throws InterruptedException {
        GraphDaoMock dao = new GraphDaoMock();
        SchedulerApiMock scheduler = new SchedulerApiMock((a, b, sch) -> {
            sch.changeStatus(b.id(), Tasks.TaskProgress.newBuilder()
                .setTid(b.id())
                .setStatus(Tasks.TaskProgress.Status.QUEUE)
                .build()
            );
            return b.id();
        });
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
        GraphWatcher watcher = new GraphWatcher(
            "", state.id(), dao, scheduler, new BfsGraphBuilder(scheduler), 100
        );
        watcher.run();

        scheduler.waitForStatus("1", Tasks.TaskProgress.Status.QUEUE);

        var state2 = dao.get("", state.id());
        Assert.assertEquals(
            Set.of("1", "3", "5", "7", "9"),
            state2.currentExecutionGroup().stream().map(t -> t.description().id()).collect(Collectors.toSet())
        );


        scheduler.changeStatus("1", SchedulerApiMock.EXECUTING);
        scheduler.changeStatus("5", SchedulerApiMock.EXECUTING);
        scheduler.changeStatus("7", SchedulerApiMock.EXECUTING);
        scheduler.changeStatus("9", SchedulerApiMock.EXECUTING);

        scheduler.waitForStatus("6", Tasks.TaskProgress.Status.QUEUE);

        var state3 = dao.get("", state.id());
        Assert.assertEquals(
            Set.of("3", "6", "10"),
            state3.currentExecutionGroup().stream().map(t -> t.description().id()).collect(Collectors.toSet())
        );
        Assert.assertEquals(GraphExecutionState.Status.EXECUTING, state3.status());

        scheduler.changeStatus("10", SchedulerApiMock.EXECUTING);
        scheduler.changeStatus("6", SchedulerApiMock.EXECUTING);

        scheduler.waitForStatus("8", Tasks.TaskProgress.Status.QUEUE);
        var state4 = dao.get("", state.id());
        Assert.assertEquals(
                Set.of("3", "8"),
                state4.currentExecutionGroup().stream().map(t -> t.description().id()).collect(Collectors.toSet())
        );

        scheduler.changeStatus("8", SchedulerApiMock.EXECUTING);
        scheduler.changeStatus("3", SchedulerApiMock.EXECUTING);

        scheduler.waitForStatus("2", Tasks.TaskProgress.Status.QUEUE);
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

        watcher.cancel();
    }

    @Test
    public void testErrorInTask() throws InterruptedException {
        GraphDaoMock dao = new GraphDaoMock();
        SchedulerApiMock scheduler = new SchedulerApiMock((a, b, sch) -> {
            sch.changeStatus(b.id(), Tasks.TaskProgress.newBuilder()
                .setTid(b.id())
                .setStatus(Tasks.TaskProgress.Status.QUEUE)
                .build()
            );
            return b.id();
        });
        final GraphDescription graph = new BfsGraphBuilderTest.GraphDescriptionBuilder()
            .addVertexes("1", "2", "3")
            .addEdge("1", "2")
            .addEdge("1", "3")
            .build();

        GraphExecutionState state = dao.create("", graph);
        GraphWatcher watcher = new GraphWatcher(
            "", state.id(), dao, scheduler, new BfsGraphBuilder(scheduler), 100
        );
        watcher.run();

        scheduler.waitForStatus("1", Tasks.TaskProgress.Status.QUEUE);
        scheduler.changeStatus("1", SchedulerApiMock.EXECUTING);

        scheduler.waitForStatus("2", Tasks.TaskProgress.Status.QUEUE);
        scheduler.changeStatus("2", SchedulerApiMock.EXECUTING);
        scheduler.changeStatus("3", SchedulerApiMock.EXECUTING);

        scheduler.changeStatus("1", SchedulerApiMock.ERROR);

        scheduler.waitForStatus("2", Tasks.TaskProgress.Status.ERROR); // wait for kill from executor

        Assert.assertEquals(dao.get("", state.id()).status(), GraphExecutionState.Status.FAILED);

    }
}
