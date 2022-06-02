package ru.yandex.cloud.ml.platform.lzy.graph.test;

import static ru.yandex.cloud.ml.platform.lzy.graph.test.GraphExecutorTest.*;

import io.micronaut.context.ApplicationContext;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.graph.db.GraphExecutionDao;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;

public class DaoTest {
    private GraphExecutionDao dao;

    @Before
    public void setUp() {
        ApplicationContext context = ApplicationContext.run();
        dao = context.getBean(GraphExecutionDao.class);
    }

    @Test
    public void daoSimpleTest() throws GraphExecutionDao.GraphDaoException {
        GraphDescription d = new GraphDescriptionBuilder()
            .addEdge("1", "2")
            .addEdge("2", "3")
            .addEdge("3", "1")
            .build();
        GraphExecutionState s = dao.create("1", d);
        GraphExecutionState s2 = dao.get("1", s.id());
        Assert.assertEquals(s, s2);

        GraphExecutionState s3 = dao.create("1", d);
        List<GraphExecutionState> list = dao.list("1");
        Assert.assertEquals(Set.of(s, s3), Set.copyOf(list));

        dao.updateAtomic("1", s.id(), t -> new GraphExecutionState(
            t.workflowId(),
            t.id(),
            t.description(),
            t.executions(),
            t.currentExecutionGroup(),
            GraphExecutionState.Status.EXECUTING
        ));

        List<GraphExecutionState> filter = dao.filter(GraphExecutionState.Status.WAITING);
        Assert.assertEquals(List.of(s3), filter);
    }

    @Test
    public void daoAtomicTest() throws GraphExecutionDao.GraphDaoException, InterruptedException {
        GraphDescription d = new GraphDescriptionBuilder()
            .addEdge("1", "2")
            .addEdge("2", "3")
            .addEdge("3", "1")
            .build();
        GraphExecutionState s = dao.create("1", d);

        AtomicBoolean firstExecuted = new AtomicBoolean(false);
        CountDownLatch firstStarted = new CountDownLatch(1);
        Thread t1 = new Thread(() -> {
            try {
                dao.updateAtomic("1", s.id(), state -> {
                    firstStarted.countDown();
                    Thread.sleep(100); // Sleep to emulate execution
                    firstExecuted.set(true);
                    return state;
                });
            } catch (GraphExecutionDao.GraphDaoException e) {
                e.printStackTrace();
            }
        });
        t1.start();
        firstStarted.await();
        dao.updateAtomic("1", s.id(), state -> {
            Assert.assertTrue(firstExecuted.get());
            return state;
        });
        t1.join();
    }
}
