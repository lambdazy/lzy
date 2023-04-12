package ai.lzy.allocator.test.dao;

import ai.lzy.allocator.alloc.dao.DynamicMountDao;
import ai.lzy.allocator.alloc.dao.SessionDao;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.model.CachePolicy;
import ai.lzy.allocator.model.DiskVolumeDescription;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.NFSVolumeDescription;
import ai.lzy.allocator.model.Session;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.test.DatabaseTestUtils;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class DynamicDaoImplTest {

    private static final String CLUSTER_ID = "1";

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private DynamicMountDao dynamicMountDao;
    private VmDao vmDao;
    private SessionDao sessionDao;
    private OperationDao operationDao;
    private Operation operation;
    private Session session;
    private Vm vm;

    @Before
    public void before() throws Exception {
        context = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("allocator", db.getConnectionInfo()));
        dynamicMountDao = context.getBean(DynamicMountDao.class);
        vmDao = context.getBean(VmDao.class);
        sessionDao = context.getBean(SessionDao.class);
        operationDao = context.getBean(OperationDao.class);

        addDefaultData();
    }

    private void addDefaultData() throws Exception {
        operation = new Operation(UUID.randomUUID().toString(), "system", Instant.now(), "op",
            Instant.now().plusSeconds(3600), null, null, Instant.now(), false,
            null, null);
        operationDao.create(operation, null);

        session = new Session(UUID.randomUUID().toString(), "system", "session",
            new CachePolicy(Duration.ZERO), operation.id());
        sessionDao.create(session, null);
        this.vm = prepareVm(session.sessionId(), operation);
    }

    private Vm prepareVm(String sessionId, Operation operation1) throws SQLException {
        var vmSpec = new Vm.Spec(UUID.randomUUID().toString(), sessionId, "s", "a", List.of(),
            List.of(), List.of(), null, ClusterRegistry.ClusterType.User);
        var allocState = new Vm.AllocateState(operation1.id(), Instant.now(), Instant.now(), "allocator",
            "foo", "ott");
        return vmDao.create(vmSpec, allocState, null);
    }

    @After
    public void tearDown() {
        context.getBean(AllocatorDataSource.class).setOnClose(DatabaseTestUtils::cleanup);
        context.stop();
    }

    @Test
    public void createAndGet() throws Exception {
        var diskMount = dynamicMountModel(vm.vmId(), "allocator", operation.id());
        var nfsMount = DynamicMount.createNew(vm.vmId(), "2", "nfs",
            "nfs", new NFSVolumeDescription("42", "nfs", "nfs-42", "share", 42,
                List.of("foo", "bar")),
            operation.id(), "allocator");

        dynamicMountDao.create(diskMount, null);
        dynamicMountDao.create(nfsMount, null);

        var fetchedDiskMount = dynamicMountDao.get(diskMount.id(), false, null);
        var fetchedNfsMount = dynamicMountDao.get(nfsMount.id(), false, null);

        Assert.assertEquals(diskMount, fetchedDiskMount);
        Assert.assertEquals(nfsMount, fetchedNfsMount);
    }

    @Test
    public void deleteAndGet() throws Exception {
        var diskMount = dynamicMountModel(vm.vmId(), "allocator", operation.id());

        dynamicMountDao.create(diskMount, null);
        dynamicMountDao.delete(diskMount.id(), null);
        var dynamicMount = dynamicMountDao.get(diskMount.id(), false, null);

        Assert.assertNull(dynamicMount);
    }

    @Test
    public void updateAndGet() throws Exception {
        var diskMount = dynamicMountModel(vm.vmId(), "allocator", operation.id());

        dynamicMountDao.create(diskMount, null);
        var exception = Assert.assertThrows(IllegalArgumentException.class, () ->
            dynamicMountDao.update(diskMount.id(), DynamicMount.Update.builder().build(), null));
        Assert.assertEquals("Update is empty", exception.getMessage());

        dynamicMountDao.update(diskMount.id(), DynamicMount.Update.builder()
            .state(DynamicMount.State.READY)
            .build(), null);
        dynamicMountDao.update(diskMount.id(), DynamicMount.Update.builder()
            .volumeName("foo")
            .volumeClaimName("bar")
            .build(), null);

        var fetchedMount = dynamicMountDao.update(diskMount.id(), DynamicMount.Update.builder()
            .unmountOperationId(operation.id())
            .build(), null);

        Assert.assertNotNull(fetchedMount);

        Assert.assertEquals(DynamicMount.State.READY, fetchedMount.state());
        Assert.assertEquals(operation.id(), fetchedMount.unmountOperationId());
        Assert.assertEquals("foo", fetchedMount.volumeName());
        Assert.assertEquals("bar", fetchedMount.volumeClaimName());
    }

    @Test
    public void getPending() throws Exception {
        var anotherVm = prepareVm(session.sessionId(), operation);
        var workerId = "allocator";
        var anotherWorkerId = "allocator-2";

        var diskMount = dynamicMountModel(vm.vmId(), workerId, operation.id());
        var anotherDiskMount = dynamicMountModel(anotherVm.vmId(), workerId, operation.id());
        var neighbourDiskMount = dynamicMountModel(anotherVm.vmId(), anotherWorkerId, operation.id());

        dynamicMountDao.create(diskMount, null);
        dynamicMountDao.create(anotherDiskMount, null);
        dynamicMountDao.create(neighbourDiskMount, null);

        var pendingMounts = dynamicMountDao.getPending(workerId, null);
        Assert.assertEquals(2, pendingMounts.size());
        Assert.assertTrue(pendingMounts.contains(diskMount));
        Assert.assertTrue(pendingMounts.contains(anotherDiskMount));

        dynamicMountDao.update(anotherDiskMount.id(), DynamicMount.Update.builder()
            .state(DynamicMount.State.READY)
            .build(), null);

        pendingMounts = dynamicMountDao.getPending(workerId, null);
        Assert.assertEquals(1, pendingMounts.size());
        Assert.assertTrue(pendingMounts.contains(diskMount));
    }

    @Test
    public void getDeleting() throws Exception {
        var anotherVm = prepareVm(session.sessionId(), operation);
        var workerId = "allocator";
        var anotherWorkerId = "allocator-2";

        var diskMount = dynamicMountModel(vm.vmId(), workerId, operation.id());
        var anotherDiskMount = dynamicMountModel(anotherVm.vmId(), workerId, operation.id());
        var neighbourDiskMount = dynamicMountModel(anotherVm.vmId(), anotherWorkerId, operation.id());

        dynamicMountDao.create(diskMount, null);
        dynamicMountDao.create(anotherDiskMount, null);
        dynamicMountDao.create(neighbourDiskMount, null);

        var deletingMounts = dynamicMountDao.getDeleting(workerId, null);
        Assert.assertTrue(deletingMounts.isEmpty());

        var update = DynamicMount.Update.builder()
            .state(DynamicMount.State.DELETING)
            .build();
        diskMount = dynamicMountDao.update(diskMount.id(), update, null);

        deletingMounts = dynamicMountDao.getDeleting(workerId, null);
        Assert.assertEquals(1, deletingMounts.size());
        Assert.assertTrue(deletingMounts.contains(diskMount));
    }

    @Test
    public void countByVolumeClaimIdWithoutMounts() throws Exception {
        var claimName = "foo";
        var count = dynamicMountDao.countVolumeClaimUsages(CLUSTER_ID, claimName, null);
        Assert.assertEquals(0, count);

        var mountOne = dynamicMountModel(vm.vmId(), "allocator", operation.id());
        var anotherClusterId = "another-cluster";
        var mountTwo = dynamicMountModel(vm.vmId(), "allocator", operation.id(), anotherClusterId);

        dynamicMountDao.create(mountOne, null);
        dynamicMountDao.create(mountTwo, null);

        dynamicMountDao.update(mountOne.id(), DynamicMount.Update.builder()
            .volumeClaimName(claimName)
            .build(), null);
        dynamicMountDao.update(mountTwo.id(), DynamicMount.Update.builder()
            .volumeClaimName(claimName)
            .build(), null);

        count = dynamicMountDao.countVolumeClaimUsages(CLUSTER_ID, claimName, null);
        Assert.assertEquals(1, count);

        count = dynamicMountDao.countVolumeClaimUsages(anotherClusterId, claimName, null);
        Assert.assertEquals(1, count);
    }

    @NotNull
    private static DynamicMount dynamicMountModel(String vmId, String workerId, String operationId) {
        return dynamicMountModel(vmId, workerId, operationId, CLUSTER_ID);
    }

    @NotNull
    private static DynamicMount dynamicMountModel(String vmId, String workerId, String operationId, String clusterId) {
        var random = UUID.randomUUID().toString();
        return DynamicMount.createNew(vmId, clusterId, "disk" + random,
            "disk" + random, new DiskVolumeDescription("42", "disk", "disk-42", 42),
            operationId, workerId);
    }
}
