package ai.lzy.graph.test;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.GraphDao;
import ai.lzy.graph.db.impl.GraphDaoImpl;
import ai.lzy.graph.model.GraphState;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
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
import java.util.EnumMap;
import java.util.List;

import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;

public class GraphDaoTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private GraphDao dao;
    private ServiceConfig config;
    private Operation operation;

    @Before
    public void setUp() throws SQLException {
        context = ApplicationContext.run(preparePostgresConfig("graph-executor-2", db.getConnectionInfo()));
        dao = context.getBean(GraphDaoImpl.class);
        config = context.getBean(ServiceConfig.class);

        var opDao = context.getBean(OperationDao.class);
        operation = Operation.create("user1", "Execute graph", null, null, null);
        opDao.create(operation, null);

        GraphState.disableLocking();
    }

    @After
    public void tearDown() {
        GraphState.enableLocking();
        context.close();
    }

    @Test
    public void daoCreateTest() throws SQLException {
        String graphId = "g1";
        GraphState graph = new GraphState(graphId, operation.id(), GraphState.Status.WAITING,
            "exec1", "workflow1", "user1", "sid1", new EnumMap<>(GraphState.Status.class), null, null, null);

        dao.create(graph, null);
        GraphState byId = dao.getById(graphId);
        GraphState wrongById = dao.getById("wrong-id");
        List<GraphState> byInstance = dao.loadActiveGraphs(config.getInstanceId());
        List<GraphState> wrongByInstance = dao.loadActiveGraphs("wrong-instance-id");

        Assert.assertEquals(graph, byId);
        Assert.assertEquals(1, byInstance.size());
        Assert.assertEquals(graph, byInstance.get(0));
        Assert.assertTrue(wrongByInstance.isEmpty());
        Assert.assertNull(wrongById);
    }


    @Test
    public void daoUpdateTest() throws SQLException {
        String graphId = "g1";
        String userId = "user1";
        String errorDescr = "error";

        GraphState graph = new GraphState(graphId, operation.id(), GraphState.Status.WAITING,
            "exec1", "workflow1", userId, "sid1", new EnumMap<>(GraphState.Status.class), null, null, null);
        dao.create(graph, null);

        graph.tryFail("id", "name", errorDescr);
        dao.update(graph, null);

        GraphState byId = dao.getById(graphId);
        List<GraphState> active = dao.loadActiveGraphs(config.getInstanceId());

        Assert.assertEquals(graph, byId);
        Assert.assertEquals(0, active.size());
        Assert.assertEquals(errorDescr, byId.errorDescription());
        Assert.assertEquals(GraphState.Status.FAILED, byId.status());
        Assert.assertEquals(userId, byId.userId());
    }
}
