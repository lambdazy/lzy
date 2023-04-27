package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.dao.DynamicMountDao;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.model.DiskVolumeDescription;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.VolumeRequest;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VolumeApi;
import com.google.protobuf.util.Durations;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class AllocatorServiceMountsTest extends AllocatorApiTestBase {

    private VmDao vmDao;
    private DynamicMountDao dynamicMountDao;

    @Before
    public void before() throws IOException {
        super.setUp();

        vmDao = allocatorCtx.getBean(VmDao.class);
        dynamicMountDao = allocatorCtx.getBean(DynamicMountDao.class);
    }

    @After
    public void after() {
        super.tearDown();
    }

    @Override
    protected void updateStartupProperties(Map<String, Object> props) {
        super.updateStartupProperties(props);
        props.put("allocator.allocation-timeout", "30s");
        props.put("allocator.kuber-tunnel-allocator.enabled", "true");
        props.put("allocator.mount.enabled", "true");
        props.put("allocator.mount.pod-image", "ubuntu");
        props.put("allocator.mount.worker-mount-point", "/mnt/worker");
        props.put("allocator.mount.host-mount-point", "/mnt/host");
    }

    @Test
    public void mountEmptyRequest() {
        var exception = Assert.assertThrows(StatusRuntimeException.class,
            () -> authorizedAllocatorBlockingStub.mount(VmAllocatorApi.MountRequest.getDefaultInstance()));
        Assert.assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
        Assert.assertTrue(exception.getMessage().contains("vm_id isn't set"));
        Assert.assertTrue(exception.getMessage().contains("mount_path isn't set"));
        Assert.assertTrue(exception.getMessage().contains("volume_type isn't set"));
    }

    @Test
    public void mountValidateVmId() {
        var exception = Assert.assertThrows(StatusRuntimeException.class,
            () -> authorizedAllocatorBlockingStub.mount(VmAllocatorApi.MountRequest.newBuilder()
                .setVmId(UUID.randomUUID().toString())
                .setMountPath("foo")
                .setDiskVolume(VolumeApi.DiskVolumeType.newBuilder()
                    .setDiskId("disk-42")
                    .setSizeGb(42)
                    .build())
                .build()));
        Assert.assertEquals(Status.NOT_FOUND.getCode(), exception.getStatus().getCode());
        Assert.assertTrue(exception.getMessage().contains("Cannot find vm"));
    }

    @Test
    public void mountValidateDeletingVm() throws Exception {
        var sessionId = createSession(Durations.ZERO);
        var vm = allocateVm(sessionId, null);
        vmDao.delete(vm.vmId(), new Vm.DeletingState(vm.allocationOpId(), "foo", "bar"), null);
        var exception = Assert.assertThrows(StatusRuntimeException.class,
            () -> authorizedAllocatorBlockingStub.mount(VmAllocatorApi.MountRequest.newBuilder()
                .setVmId(vm.vmId())
                .setMountPath("foo")
                .setDiskVolume(VolumeApi.DiskVolumeType.newBuilder()
                    .setDiskId("disk-42")
                    .setSizeGb(42)
                    .build())
                .build()));
        Assert.assertEquals(Status.FAILED_PRECONDITION.getCode(), exception.getStatus().getCode());
        Assert.assertTrue(exception.getMessage().contains("Cannot mount volume to deleting vm"));
    }

    @Test
    public void listMountsValidateVm() {
        var exception = Assert.assertThrows(StatusRuntimeException.class,
            () -> authorizedAllocatorBlockingStub.listMounts(VmAllocatorApi.ListMountsRequest.getDefaultInstance()));
        Assert.assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
        Assert.assertTrue(exception.getMessage().contains("vm_id isn't set"));
    }

    @Test
    public void listMounts() throws Exception {
        var sessionId = createSession(Durations.ZERO);
        var vm = allocateVm(sessionId, null);
        var mount1 = DynamicMount.createNew(vm.vmId(), "foo", "bar", "baz",
            new VolumeRequest("disk-1", new DiskVolumeDescription("disk-1", "1", 42)),
            vm.allocationOpId(), "allocator");
        var mount2 = DynamicMount.createNew(vm.vmId(), "foo", "qux", "quux",
            new VolumeRequest("disk-1", new DiskVolumeDescription("disk-1", "1", 42)),
            vm.allocationOpId(), "allocator");
        dynamicMountDao.create(mount1, null);
        dynamicMountDao.create(mount2, null);

        var mounts = authorizedAllocatorBlockingStub.listMounts(VmAllocatorApi.ListMountsRequest.newBuilder()
            .setVmId(vm.vmId())
            .build());
        Assert.assertEquals(2, mounts.getMountsCount());
        var fetchedMountsById = mounts.getMountsList().stream().collect(Collectors.toMap(
            VmAllocatorApi.DynamicMount::getId, x -> x));
        var fetchedMount1 = fetchedMountsById.get(mount1.id());
        Assert.assertNotNull(fetchedMount1);
        Assert.assertEquals(mount1.id(), fetchedMount1.getId());
        Assert.assertEquals(mount1.vmId(), fetchedMount1.getVmId());
        Assert.assertEquals(mount1.mountPath(), fetchedMount1.getMountPath());
        Assert.assertEquals(mount1.state().name(), fetchedMount1.getState());
        Assert.assertEquals(mount1.mountOperationId(), fetchedMount1.getMountOperationId());
        Assert.assertFalse(fetchedMount1.hasUnmountOperationId());
        Assert.assertFalse(fetchedMount1.hasVolumeName());
        Assert.assertFalse(fetchedMount1.hasVolumeClaimName());
        var diskVolumeDescription = (DiskVolumeDescription) mount1.volumeRequest().volumeDescription();
        Assert.assertEquals(diskVolumeDescription.diskId(), fetchedMount1.getVolumeRequest().getDiskVolume()
            .getDiskId());
        Assert.assertEquals(diskVolumeDescription.sizeGb(), fetchedMount1.getVolumeRequest().getDiskVolume()
            .getSizeGb());

        var fetchedMount2 = fetchedMountsById.get(mount2.id());
        Assert.assertNotNull(fetchedMount2);
        Assert.assertEquals(mount2.id(), fetchedMount2.getId());
        Assert.assertEquals(mount2.vmId(), fetchedMount2.getVmId());
        Assert.assertEquals(mount2.mountPath(), fetchedMount2.getMountPath());
        Assert.assertEquals(mount2.state().name(), fetchedMount2.getState());
        Assert.assertEquals(mount2.mountOperationId(), fetchedMount2.getMountOperationId());
        Assert.assertFalse(fetchedMount2.hasUnmountOperationId());
        Assert.assertFalse(fetchedMount2.hasVolumeName());
        Assert.assertFalse(fetchedMount2.hasVolumeClaimName());
        diskVolumeDescription = (DiskVolumeDescription) mount2.volumeRequest().volumeDescription();
        Assert.assertEquals(diskVolumeDescription.diskId(), fetchedMount2.getVolumeRequest().getDiskVolume()
            .getDiskId());
        Assert.assertEquals(diskVolumeDescription.sizeGb(), fetchedMount2.getVolumeRequest().getDiskVolume()
            .getSizeGb());
    }

}
