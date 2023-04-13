package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.dao.SessionDao;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskMeta;
import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.DiskType;
import ai.lzy.allocator.disk.dao.DiskDao;
import ai.lzy.allocator.model.*;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.time.Instant.now;

public class DaoTest {

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private OperationDao opDao;
    private SessionDao sessionDao;
    private VmDao vmDao;
    private DiskDao diskDao;
    private Storage storage;
    private ApplicationContext context;

    @Before
    public void setUp() {
        context = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("allocator", db.getConnectionInfo()));
        //context = ApplicationContext.run(DatabaseTestUtils.prepareLocalhostConfig("allocator"));
        storage = context.getBean(Storage.class);
        opDao = context.getBean(OperationDao.class, Qualifiers.byName("AllocatorOperationDao"));
        sessionDao = context.getBean(SessionDao.class);
        vmDao = context.getBean(VmDao.class);
        diskDao = context.getBean(DiskDao.class);
    }

    @After
    public void tearDown() {
        context.getBean(AllocatorDataSource.class).setOnClose(DatabaseTestUtils::cleanup);
        context.stop();
    }

    @Test
    public void testOp() throws SQLException {
        final var meta = VmAllocatorApi.AllocateMetadata.newBuilder()
            .setVmId("id")
            .build();

        final var op1 = new Operation(
            UUID.randomUUID().toString(),
            "test",
            now(),
            "Some op",
            /* deadline */ null,
            null,
            Any.pack(meta),
            now(),
            false,
            null,
            null);

        opDao.create(op1, null);

        final var op2 = opDao.get(op1.id(), null);
        Assert.assertNotNull(op2);
        Assert.assertFalse(op2.done());
        Assert.assertEquals("Some op", op2.description());

        var status = com.google.rpc.Status.newBuilder()
            .setCode(io.grpc.Status.NOT_FOUND.getCode().value())
            .setMessage("Error")
            .build();

        opDao.fail(op2.id(), status, null);

        final var op3 = opDao.get(op1.id(), null);
        Assert.assertNotNull(op3);
        Assert.assertTrue(op3.done());
        Assert.assertEquals("Error", op3.error().getDescription());
        Assert.assertEquals(io.grpc.Status.NOT_FOUND.getCode(), op3.error().getCode());
    }

    @Test
    public void testTransaction() throws SQLException {
        final var meta = VmAllocatorApi.AllocateMetadata.newBuilder()
            .setVmId("id")
            .build();

        var op = new Operation(
            UUID.randomUUID().toString(),
            "test",
            now(),
            "Some op",
            /* deadline */ null,
            null,
            Any.pack(meta),
            now(),
            false,
            null,
            null);

        try (final var tx = TransactionHandle.create(storage)) {
            opDao.create(op, tx);
            // Do not commit
        }

        final var op1 = opDao.get(op.id(), null);
        Assert.assertNull(op1);

        op = Operation.create("test", "Some op", null, meta);
        try (final var tx = TransactionHandle.create(storage)) {
            opDao.create(op, tx);
            tx.commit();
        }

        final var op2 = opDao.get(op.id(), null);
        Assert.assertNotNull(op2);
    }

    @Test
    public void testSession() throws SQLException {
        Session s = createSession();

        final var s1 = sessionDao.get(s.sessionId(), null);
        Assert.assertNotNull(s1);
        Assert.assertEquals("owner", s1.owner());
        Assert.assertEquals(Duration.ofSeconds(10), s1.cachePolicy().minIdleTimeout());

        sessionDao.delete(s.sessionId(), s.createOpId(), "reqid-1", null);
        Assert.assertNull(sessionDao.get(s.sessionId(), null));
    }

    private Session createSession() throws SQLException {
        var opId = UUID.randomUUID().toString();
        var op = Operation.createCompleted(opId, "owner", "descr", null, null, Empty.getDefaultInstance());
        var sid = UUID.randomUUID().toString();
        var s = new Session(sid, "owner", "descr", new CachePolicy(Duration.ofSeconds(10)), opId);

        try (var tx = TransactionHandle.create(storage)) {
            opDao.create(op, tx);
            sessionDao.create(s, tx);
            tx.commit();
        }
        return s;
    }

    @Test
    public void testVm() throws SQLException {
        var session = createSession();
        var allocOp = Operation.createCompleted("xxx", "owner", "descr", null, null,
            VmAllocatorApi.AllocateResponse.getDefaultInstance());
        opDao.create(allocOp, null);

        final VolumeMount volume = new VolumeMount(
            "volume", "/mnt/volume", false, VolumeMount.MountPropagation.BIDIRECTIONAL);
        final var wl1 = new Workload(
            "wl1", "im", Map.of("a", "b"), List.of("a1", "a2"), Map.of(1111, 2222),
            List.of(volume));
        final var volumeRequest = new VolumeRequest(new DiskVolumeDescription("disk-some-volume-name", "diskId", 3));

        final var vmSpec = new Vm.Spec(
            "placeholder",
            session.sessionId(),
            "pool",
            "zone",
            List.of(),
            List.of(wl1),
            List.of(volumeRequest),
            null,
            ClusterRegistry.ClusterType.User);
        final var vmAllocState = new Vm.AllocateState(
            allocOp.id(),
            now(),
            now().plus(Duration.ofDays(1)),
            "worker",
            "reqid",
            "ott",
            null,
            null);
        final var vm = vmDao.create(vmSpec, vmAllocState, null);

        final var vm1 = vmDao.get(vm.vmId(), null);
        Assert.assertNotNull(vm1);
        Assert.assertEquals(session.sessionId(), vm1.sessionId());
        Assert.assertEquals("pool", vm1.poolLabel());
        Assert.assertEquals("zone", vm1.zone());
        Assert.assertEquals(List.of(wl1), vm1.workloads());
        Assert.assertEquals(allocOp.id(), vm1.allocOpId());
        Assert.assertEquals(Vm.Status.ALLOCATING, vm1.status());
        Assert.assertEquals(List.of(volumeRequest), vm1.volumeRequests());

        vmDao.setVmRunning(vm1.vmId(), Map.of(), now().plus(Duration.ofDays(1)), null);
        vmDao.release(vm1.vmId(), now().plus(Duration.ofHours(1)), null);

        var vm2 = vmDao.acquire(vmSpec, null);
        Assert.assertNotNull(vm2);
        Assert.assertEquals(vm1.vmId(), vm2.vmId());
        Assert.assertEquals(Vm.Status.IDLE, vm2.status()); // `acquire` returns previous state

        vm2 = vmDao.get(vm2.vmId(), null);
        Assert.assertEquals(Vm.Status.RUNNING, vm2.status());

        final var vms = vmDao.getSessionVms(session.sessionId(), null);
        Assert.assertEquals(List.of(vm2), vms);

        vmDao.release(vm2.vmId(), now().minus(Duration.ofSeconds(10)), null);

        final var vms2 = vmDao.listExpiredVms(100);
        Assert.assertEquals(1, vms2.size());
        Assert.assertEquals(vm.vmId(), vms2.get(0).vmId());

        final var meta = Map.of("a", "b", "c", "d");

        Assert.assertNull(vmDao.getAllocatorMeta(vm.vmId(), null));

        vmDao.setAllocatorMeta(vm.vmId(), meta, null);
        Assert.assertEquals(meta, vmDao.getAllocatorMeta(vm.vmId(), null));
    }

    @Test
    public void testDiskCreateRemove() throws SQLException {
        final Disk disk = new Disk(
            "disk-id",
            new DiskSpec("disk-name", DiskType.HDD, 35, "ru-central1-a"),
            new DiskMeta("user-id")
        );
        diskDao.insert(disk, null);

        final Disk getDisk = diskDao.get(disk.id(), null);
        Assert.assertEquals(disk, getDisk);

        diskDao.remove(disk.id(), null);

        Assert.assertNull(diskDao.get(disk.id(), null));
    }
}
