package ai.lzy.scheduler.test;

import ai.lzy.model.db.DaoException;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.scheduler.allocator.ServantMetaStorage;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.db.ServantDao.AcquireException;
import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.servant.Servant;
import io.micronaut.context.ApplicationContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

public class ServantDaoTest {
    public static final ApplicationContext context = ApplicationContext.run();
    public static final ServantDao dao = context.getBean(ServantDao.class);
    public static final ServantMetaStorage meta = context.getBean(ServantMetaStorage.class);

    @After
    public void tearDown() throws DaoException {
        for (Servant servant : dao.getAllFree()) {
            dao.invalidate(servant, "destroy");
        }
        for (Servant servant : dao.getAllAcquired()) {
            dao.invalidate(servant, "destroy");
        }
    }

    @Test
    public void testSimple() throws DaoException, AcquireException {
        var servant = dao.create("wf", new Provisioning.Any());
        Assert.assertNotNull(servant);
        Assert.assertEquals(servant.status(), ServantState.Status.CREATED);

        var state = dao.acquire(servant.workflowName(), servant.id());
        Assert.assertNotNull(state);
        dao.updateAndFree(
            state.copy()
                .setStatus(ServantState.Status.DESTROYED)
                .build()
        );
        var newServant = dao.get(servant.workflowName(), servant.id());
        Assert.assertNotNull(newServant);
        Assert.assertEquals(newServant.status(), ServantState.Status.DESTROYED);
    }

    @Test
    public void testAcquire() throws DaoException, AcquireException {
        var servant = dao.create("wf", new Provisioning.Any());
        var state = dao.acquire(servant.workflowName(), servant.id());

        Assert.assertThrows(AcquireException.class, () -> dao.acquire(servant.workflowName(), servant.id()));
        dao.updateAndFree(state);

        dao.acquire(servant.workflowName(), servant.id());
    }

    @Test
    public void testAcquireForTask() throws DaoException, AcquireException {
        var servant = dao.create("wf", new Provisioning.Any());
        dao.acquireForTask(servant.workflowName(), servant.id());

        Assert.assertThrows(AcquireException.class, () -> dao.acquireForTask(servant.workflowName(), servant.id()));
        dao.freeFromTask(servant.workflowName(), servant.id());
        dao.acquireForTask(servant.workflowName(), servant.id());
    }

    @Test
    public void testCount() throws DaoException, AcquireException {
        var servant1 = dao.create("wf", new Provisioning.Any());
        var servant2 = dao.create("wf", new Provisioning.Any());
        var servant3 = dao.create("wf", new Provisioning.Any());

        dao.acquire(servant1.workflowName(), servant1.id());

        var free = dao.getAllFree();
        var acquired = dao.getAllAcquired();

        Assert.assertEquals(2, free.size());
        Assert.assertEquals(1, acquired.size());

        Assert.assertEquals(servant1.id(), acquired.get(0).id());
        Assert.assertEquals(Set.of(servant2.id(), servant3.id()),
            free.stream().map(Servant::id).collect(Collectors.toSet()));
    }

    @Test
    public void testInvalidate() throws DaoException, AcquireException {
        var servant1 = dao.create("wf", new Provisioning.Any());
        var servant2 = dao.create("wf", new Provisioning.Any());
        var servant3 = dao.create("wf", new Provisioning.Any());

        var all = dao.get("wf");

        Assert.assertEquals(3, all.size());

        dao.invalidate(servant1, "Destroy");
        dao.invalidate(servant2, "Destroy");

        all = dao.get("wf");

        Assert.assertEquals(1, all.size());

        var s1 = dao.get(servant1.workflowName(), servant1.id());
        Assert.assertEquals("Destroy", s1.errorDescription());
    }

    @Test
    public void metaTest() throws DaoException {
        var servant1 = dao.create("wf", new Provisioning.Any());
        meta.saveMeta(servant1.workflowName(), servant1.id(), "Meta");
        var metadata = meta.getMeta(servant1.workflowName(), servant1.id());
        Assert.assertNotNull(metadata);
        Assert.assertEquals("Meta", metadata);
        meta.clear(servant1.workflowName(), servant1.id());
        Assert.assertNull(meta.getMeta(servant1.workflowName(), servant1.id()));
    }
}
