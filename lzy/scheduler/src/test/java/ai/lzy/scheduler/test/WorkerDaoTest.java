package ai.lzy.scheduler.test;

import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.model.operation.Operation;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

public class WorkerDaoTest {
//    @Rule
//    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});
//
//    public ApplicationContext context;
//    public WorkerDao dao;
//    public WorkerMetaStorage meta;
//
//    @Before
//    public void setUp() {
//        context = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("scheduler", db.getConnectionInfo()));
//        dao = context.getBean(WorkerDao.class);
//        meta = context.getBean(WorkerMetaStorage.class);
//    }
//
//    @After
//    public void tearDown() throws DaoException {
//        for (Worker worker : dao.getAllFree()) {
//            dao.invalidate(worker, "destroy");
//        }
//        for (Worker worker : dao.getAllAcquired()) {
//            dao.invalidate(worker, "destroy");
//        }
//        context.close();
//    }
//
//    @Test
//    public void testSimple() throws DaoException, AcquireException {
//        var worker = dao.create("uid", "wf", new Operation.Requirements("s", "a"));
//        Assert.assertNotNull(worker);
//        Assert.assertEquals(worker.status(), WorkerState.Status.CREATED);
//
//        var state = dao.acquire(worker.workflowName(), worker.id());
//        Assert.assertNotNull(state);
//        dao.updateAndFree(
//            state.copy()
//                .setStatus(WorkerState.Status.DESTROYED)
//                .build()
//        );
//        var newWorker = dao.get(worker.workflowName(), worker.id());
//        Assert.assertNotNull(newWorker);
//        Assert.assertEquals(newWorker.status(), WorkerState.Status.DESTROYED);
//    }
//
//    @Test
//    public void testAcquire() throws DaoException, AcquireException {
//        var worker = dao.create("uid", "wf", new Operation.Requirements("s", "a"));
//        var state = dao.acquire(worker.workflowName(), worker.id());
//
//        Assert.assertThrows(AcquireException.class, () -> dao.acquire(worker.workflowName(), worker.id()));
//        dao.updateAndFree(state);
//
//        dao.acquire(worker.workflowName(), worker.id());
//    }
//
//    @Test
//    public void testAcquireForTask() throws DaoException, AcquireException {
//        var worker = dao.create("uid", "wf", new Operation.Requirements("s", "a"));
//        dao.acquireForTask(worker.workflowName(), worker.id());
//
//        Assert.assertThrows(AcquireException.class, () -> dao.acquireForTask(worker.workflowName(), worker.id()));
//        dao.freeFromTask(worker.workflowName(), worker.id());
//        dao.acquireForTask(worker.workflowName(), worker.id());
//    }
//
//    @Test
//    public void testCount() throws DaoException, AcquireException {
//        var worker1 = dao.create("uid", "wf", new Operation.Requirements("s", "a"));
//        var worker2 = dao.create("uid", "wf", new Operation.Requirements("s", "a"));
//        var worker3 = dao.create("uid", "wf", new Operation.Requirements("s", "a"));
//
//        dao.acquire(worker1.workflowName(), worker1.id());
//
//        var free = dao.getAllFree();
//        var acquired = dao.getAllAcquired();
//
//        Assert.assertEquals(2, free.size());
//        Assert.assertEquals(1, acquired.size());
//
//        Assert.assertEquals(worker1.id(), acquired.get(0).id());
//        Assert.assertEquals(Set.of(worker2.id(), worker3.id()),
//            free.stream().map(Worker::id).collect(Collectors.toSet()));
//    }
//
//    @Test
//    public void testInvalidate() throws DaoException, AcquireException {
//        var worker1 = dao.create("uid", "wf", new Operation.Requirements("s", "a"));
//        var worker2 = dao.create("uid", "wf", new Operation.Requirements("s", "a"));
//        var worker3 = dao.create("uid", "wf", new Operation.Requirements("s", "a"));
//
//        var all = dao.get("wf");
//
//        Assert.assertEquals(3, all.size());
//
//        dao.invalidate(worker1, "Destroy");
//        dao.invalidate(worker2, "Destroy");
//
//        all = dao.get("wf");
//
//        Assert.assertEquals(1, all.size());
//
//        var s1 = dao.get(worker1.workflowName(), worker1.id());
//        Assert.assertEquals("Destroy", s1.errorDescription());
//    }
//
//    @Test
//    public void metaTest() throws DaoException {
//        var worker1 = dao.create("uid", "wf", new Operation.Requirements("s", "a"));
//        meta.saveMeta(worker1.workflowName(), worker1.id(), "Meta");
//        var metadata = meta.getMeta(worker1.workflowName(), worker1.id());
//        Assert.assertNotNull(metadata);
//        Assert.assertEquals("Meta", metadata);
//        meta.clear(worker1.workflowName(), worker1.id());
//        Assert.assertNull(meta.getMeta(worker1.workflowName(), worker1.id()));
//    }
}
