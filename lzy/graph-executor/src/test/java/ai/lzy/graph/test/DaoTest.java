package ai.lzy.graph.test;

import ai.lzy.graph.db.GraphExecutionDao;
import ai.lzy.graph.db.impl.GraphExecutionDaoImpl;
import ai.lzy.graph.model.GraphDescription;
import ai.lzy.graph.model.GraphExecutionState;
import ai.lzy.model.db.exceptions.DaoException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static ai.lzy.graph.test.GraphExecutorTest.GraphDescriptionBuilder;
import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;

public class DaoTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(20);
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private GraphExecutionDao dao;

    @Before
    public void setUp() {
        context = ApplicationContext.run(preparePostgresConfig("graph-executor", db.getConnectionInfo()));
        dao = context.getBean(GraphExecutionDaoImpl.class);
    }

    @After
    public void tearDown() {
        context.close();
    }

    @Test
    public void daoSimpleTest() throws SQLException, DaoException {
        GraphDescription d = new GraphDescriptionBuilder()
            .addEdge("1", "2")
            .addEdge("2", "3")
            .addEdge("3", "1")
            .build();
        GraphExecutionState s = dao.create("1", "changeMe", "userId", "allocSid", d, null);
        GraphExecutionState s2 = dao.get("1", s.id());
        Assert.assertEquals(s, s2);

        GraphExecutionState s3 = dao.create("1", "changeMe", "userId", "allocSid", d, null);
        List<GraphExecutionState> list = dao.list("1");
        Assert.assertEquals(Set.of(s, s3), Set.copyOf(list));

        GraphExecutionState s4 = dao.acquire("1", s.id());
        Assert.assertNotNull(s4);

        Assert.assertThrows(DaoException.class,
            () -> dao.acquire("1", s.id()));

        dao.updateAndFree(
            s4.copyFromThis()
                .withStatus(GraphExecutionState.Status.EXECUTING)
                .build()
        );

        List<GraphExecutionState> filter = dao.filter(GraphExecutionState.Status.WAITING);
        Assert.assertEquals(List.of(s3), filter);
    }
}
