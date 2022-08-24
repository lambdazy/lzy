package ai.lzy.allocator.test;

import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.SessionDao;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import ai.lzy.allocator.model.CachePolicy;
import ai.lzy.allocator.model.Operation;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandleImpl;
import ai.lzy.model.db.test.DatabaseCleaner;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.micronaut.context.ApplicationContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class DaoTest {
    private OperationDao opDao;
    private SessionDao sessionDao;
    private VmDao vmDao;
    private Storage storage;
    private ApplicationContext context;

    @Before
    public void setUp() {
        context = ApplicationContext.run();
        opDao = context.getBean(OperationDao.class);
        storage = context.getBean(Storage.class);
        sessionDao = context.getBean(SessionDao.class);
        vmDao = context.getBean(VmDao.class);
    }

    @After
    public void tearDown() {
        DatabaseCleaner.cleanup(context.getBean(AllocatorDataSource.class));
        context.stop();
    }

    @Test
    public void testOp() {
        final var meta = VmAllocatorApi.AllocateMetadata.newBuilder()
            .setVmId("id")
            .build();
        final var op1 = opDao.create("Some op", "test", Any.pack(meta), null);

        final var op2 = opDao.get(op1.id(), null);
        Assert.assertNotNull(op2);
        Assert.assertFalse(op2.done());
        Assert.assertEquals("Some op", op2.description());

        opDao.update(op2.complete(Status.NOT_FOUND.withDescription("Error")), null);
        final var op3 = opDao.get(op1.id(), null);
        Assert.assertNotNull(op3);
        Assert.assertTrue(op3.done());
        Assert.assertEquals("Error", op3.error().getDescription());
        Assert.assertEquals(Status.NOT_FOUND.getCode(), op3.error().getCode());
    }

    @Test
    public void testTransaction() throws SQLException {
        final var meta = VmAllocatorApi.AllocateMetadata.newBuilder()
            .setVmId("id")
            .build();
        Operation op;
        try (final var tx = new TransactionHandleImpl(storage)) {
            op = opDao.create("Some op", "test", Any.pack(meta), tx);
            // Do not commit
        }

        final var op1 = opDao.get(op.id(), null);
        Assert.assertNull(op1);

        try (final var tx = new TransactionHandleImpl(storage)) {
            op = opDao.create("Some op", "test", Any.pack(meta), tx);
            tx.commit();
        }

        final var op2 = opDao.get(op.id(), null);
        Assert.assertNotNull(op2);
    }

    @Test
    public void testSession() {
        final var s = sessionDao.create("test", new CachePolicy(Duration.ofSeconds(10)), null);

        final var s1 = sessionDao.get(s.sessionId(), null);
        Assert.assertNotNull(s1);
        Assert.assertEquals("test", s1.owner());
        Assert.assertEquals(Duration.ofSeconds(10), s1.cachePolicy().minIdleTimeout());

        sessionDao.delete(s.sessionId(), null);
        Assert.assertNull(sessionDao.get(s.sessionId(), null));
    }

    @Test
    public void testVm() {
        final var wl1 = new Workload("wl1", "im", Map.of("a", "b"), List.of("a1", "a2"), Map.of(1111, 2222));
        final var vm = vmDao.create("session", "pool", "zone", List.of(wl1), "op1", null);

        final var vm1 = vmDao.get(vm.vmId(), null);
        Assert.assertNotNull(vm1);
        Assert.assertEquals("session", vm1.sessionId());
        Assert.assertEquals("pool", vm1.poolLabel());
        Assert.assertEquals("zone", vm1.zone());
        Assert.assertEquals(List.of(wl1), vm1.workloads());
        Assert.assertEquals("op1", vm1.allocationOperationId());
        Assert.assertEquals(Vm.State.CREATED, vm1.state());

        vmDao.update(new Vm.VmBuilder(vm1).setState(Vm.State.IDLE).build(), null);
        final var vm2 = vmDao.acquire("session", "pool", "zone", null);
        Assert.assertNotNull(vm2);
        Assert.assertEquals(vm1.vmId(), vm2.vmId());
        Assert.assertEquals(Vm.State.RUNNING, vm2.state());

        final var vms = vmDao.list("session", null);
        Assert.assertEquals(List.of(vm2), vms);

        final var vm3 = new Vm.VmBuilder(vm2)
            .setState(Vm.State.IDLE)
            .setDeadline(Instant.now().minus(Duration.ofSeconds(1)))
            .build();

        vmDao.update(vm3, null);

        final var vms2 = vmDao.getExpired(100, null);
        Assert.assertEquals(vm.vmId(), vms2.get(0).vmId());

        final var meta = Map.of("a", "b", "c", "d");

        Assert.assertNull(vmDao.getAllocatorMeta(vm.vmId(), null));

        vmDao.saveAllocatorMeta(vm.vmId(), meta, null);
        Assert.assertEquals(meta, vmDao.getAllocatorMeta(vm.vmId(), null));
    }
}
