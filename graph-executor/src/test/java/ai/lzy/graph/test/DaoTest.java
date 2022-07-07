package ai.lzy.graph.test;

import static ai.lzy.graph.test.GraphExecutorTest.*;

import ai.lzy.graph.db.impl.GraphExecutionDaoImpl;
import io.micronaut.context.ApplicationContext;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import ai.lzy.graph.db.GraphExecutionDao;
import ai.lzy.graph.db.DaoException;
import ai.lzy.graph.model.GraphDescription;
import ai.lzy.graph.model.GraphExecutionState;
import org.junit.rules.Timeout;

public class DaoTest {
    private GraphExecutionDao dao;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    @Before
    public void setUp() {
        ApplicationContext context = ApplicationContext.run();
        dao = context.getBean(GraphExecutionDaoImpl.class);
    }

    @Test
    public void daoSimpleTest() throws DaoException {
        GraphDescription d = new GraphDescriptionBuilder()
            .addEdge("1", "2")
            .addEdge("2", "3")
            .addEdge("3", "1")
            .build();
        GraphExecutionState s = dao.create("1", "changeMe", d);
        GraphExecutionState s2 = dao.get("1", s.id());
        Assert.assertEquals(s, s2);

        GraphExecutionState s3 = dao.create("1", "changeMe", d);
        List<GraphExecutionState> list = dao.list("1");
        Assert.assertEquals(Set.of(s, s3), Set.copyOf(list));

        GraphExecutionState s4 = dao.acquire("1", s.id());
        Assert.assertNotNull(s4);

        Assert.assertThrows(DaoException.class,
            () -> dao.acquire("1", s.id()));

        dao.free(
            s4.copyFromThis()
            .withStatus(GraphExecutionState.Status.EXECUTING)
            .build()
        );

        List<GraphExecutionState> filter = dao.filter(GraphExecutionState.Status.WAITING);
        Assert.assertEquals(List.of(s3), filter);
    }
}
