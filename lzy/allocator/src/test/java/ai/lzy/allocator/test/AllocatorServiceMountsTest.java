package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.dao.DynamicMountDao;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.alloc.impl.kuber.KuberMountHolderManager;
import ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator;
import ai.lzy.allocator.model.*;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VolumeApi;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.util.Durations;
import io.fabric8.kubernetes.api.model.PersistentVolume;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import io.fabric8.kubernetes.client.server.mock.OutputStreamMessage;
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

import static java.util.Objects.requireNonNull;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class AllocatorServiceMountsTest extends AllocatorApiTestBase {

    public static final String WORKER_MOUNT_POINT = "/mnt/worker";
    public static final String HOST_MOUNT_POINT = "/mnt/host";
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
        props.put("allocator.mount-timeout", "30s");
        props.put("allocator.mount.enabled", "true");
        props.put("allocator.mount.pod-image", "ubuntu");
        props.put("allocator.mount.worker-mount-point", WORKER_MOUNT_POINT);
        props.put("allocator.mount.host-mount-point", HOST_MOUNT_POINT);
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
            () -> mountDisk(UUID.randomUUID().toString(), "foo", "disk-42", 42));
        Assert.assertEquals(Status.NOT_FOUND.getCode(), exception.getStatus().getCode());
        Assert.assertTrue(exception.getMessage().contains("Cannot find vm"));
    }

    @Test
    public void mountValidateDeletingVm() throws Exception {
        var sessionId = createSession(Durations.ZERO);
        var vm = allocateWithMountPod(sessionId);
        vmDao.delete(vm.vmId(), new Vm.DeletingState(vm.allocationOpId(), "foo", "bar"), null);
        var exception = Assert.assertThrows(StatusRuntimeException.class,
            () -> mountDisk(vm.vmId(), "foo", "disk-42", 42));
        Assert.assertEquals(Status.FAILED_PRECONDITION.getCode(), exception.getStatus().getCode());
        Assert.assertTrue(exception.getMessage().contains("Cannot mount volume to deleting vm"));
    }

    @Test
    public void mountDuplicateMount() throws Exception {
        var sessionId = createSession(Durations.ZERO);
        var vm = allocateWithMountPod(sessionId);
        var dynamicMount = DynamicMount.createNew(vm.vmId(), "foo", "disk-disk-42",
            WORKER_MOUNT_POINT + "/foo", new VolumeRequest(UUID.randomUUID().toString(),
                new DiskVolumeDescription(UUID.randomUUID().toString(), "disk-42", 42)), vm.allocationOpId(),
            "allocator");
        dynamicMountDao.create(dynamicMount, null);
        var exception = Assert.assertThrows(StatusRuntimeException.class,
            () -> mountDisk(vm.vmId(), "foo", "disk-42", 42));
        Assert.assertEquals(Status.ALREADY_EXISTS.getCode(), exception.getStatus().getCode());
        Assert.assertTrue(exception.getMessage().contains("Mount with path /mnt/worker/foo already exists"));
        Assert.assertTrue(exception.getMessage().contains("Mount with name disk-disk-42 already exists"));
        Assert.assertTrue(exception.getMessage().contains("Disk disk-42 is already mounted"));
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

    @Test
    public void mountTest() throws Exception {
        var sessionId = createSession(Durations.fromDays(10));
        var allocatedVm = allocateWithMountPod(sessionId);
        var vm = vmDao.get(allocatedVm.vmId(), null);
        Assert.assertNotNull(vm);
        var mountPodName = vm.instanceProperties().mountPodName();
        Assert.assertNotNull(mountPodName);

        var pv = awaitResourceCreate(PersistentVolume.class, PERSISTENT_VOLUME_PATH);
        var pvc = awaitResourceCreate(PersistentVolumeClaim.class, PERSISTENT_VOLUME_CLAIM_PATH);
        mockDeletePodByName(mountPodName, 200);
        var updatedMountPod = mockCreatePod();
        updatedMountPod.thenAccept(pod -> {
            pod.setStatus(new PodStatusBuilder()
                .withPhase(PodPhase.RUNNING.getPhase())
                .build());
            mockGetPodByName(pod);
        });
        var mountOp = mountDisk(vm.vmId(), "foo", "disk-42", 1);

        waitOpSuccess(mountOp);
        var mountMetadata = mountOp.getMetadata().unpack(VmAllocatorApi.MountMetadata.class);
        Assert.assertTrue(mountMetadata.hasMount());
        var dynamicMount = mountMetadata.getMount();
        Assert.assertEquals(vm.vmId(), dynamicMount.getVmId());
        Assert.assertEquals(WORKER_MOUNT_POINT + "/foo", dynamicMount.getMountPath());
        Assert.assertEquals("disk-42", dynamicMount.getVolumeRequest().getDiskVolume().getDiskId());
        Assert.assertEquals(1, dynamicMount.getVolumeRequest().getDiskVolume().getSizeGb());
        Assert.assertEquals(mountOp.getId(), dynamicMount.getMountOperationId());
        Assert.assertEquals("PENDING", dynamicMount.getState());
        Assert.assertFalse(dynamicMount.hasUnmountOperationId());
        Assert.assertFalse(dynamicMount.hasVolumeName());
        Assert.assertFalse(dynamicMount.hasVolumeClaimName());

        var updatedMount = dynamicMountDao.get(dynamicMount.getId(), false, null);

        var clusterId = getClusterId(vm);
        Assert.assertEquals(vm.vmId(), updatedMount.vmId());
        Assert.assertEquals(clusterId, updatedMount.clusterId());
        Assert.assertEquals(WORKER_MOUNT_POINT + "/foo", updatedMount.mountPath());
        Assert.assertEquals(getName(pv.get()), updatedMount.volumeName());
        Assert.assertEquals(getName(pvc.get()), updatedMount.volumeClaimName());
        var volumeDescription = updatedMount.volumeRequest().volumeDescription();
        Assert.assertTrue(volumeDescription instanceof DiskVolumeDescription);
        var diskVolumeDescription = (DiskVolumeDescription) volumeDescription;
        Assert.assertEquals("disk-42", diskVolumeDescription.diskId());
        Assert.assertEquals(1, diskVolumeDescription.sizeGb());
        Assert.assertEquals(mountOp.getId(), updatedMount.mountOperationId());
        Assert.assertEquals(DynamicMount.State.READY, updatedMount.state());
        Assert.assertNull(updatedMount.unmountOperationId());

        var updatedVm = vmDao.get(vm.vmId(), null);
        Assert.assertEquals(getName(updatedMountPod.get()), updatedVm.instanceProperties().mountPodName());
    }

    @Test
    public void unmountTest() throws Exception {
        var sessionId = createSession(Durations.fromDays(10));
        var allocatedVm = allocateWithMountPod(sessionId);
        var vm = vmDao.get(allocatedVm.vmId(), null);
        Assert.assertNotNull(vm);
        var mountPodName = vm.instanceProperties().mountPodName();
        Assert.assertNotNull(mountPodName);

        var mountPath = WORKER_MOUNT_POINT + "/foo";
        var dynamicMount = DynamicMount.createNew(allocatedVm.vmId(), getClusterId(vm), "disk-42",
            mountPath, new VolumeRequest(UUID.randomUUID().toString(),
                new DiskVolumeDescription(UUID.randomUUID().toString(), "disk-42", 42)),
            allocatedVm.allocationOpId(), "allocator-0");
        dynamicMountDao.create(dynamicMount, null);
        var volumeClaimName = "claim-42";
        var volumeName = "volume-42";
        var update = DynamicMount.Update.builder()
            .volumeClaimName(volumeClaimName)
            .volumeName(volumeName)
            .state(DynamicMount.State.READY)
            .build();
        dynamicMountDao.update(dynamicMount.id(), update, null);

        mockDeletePodByName(mountPodName, 200);
        var updatedMountPod = mockCreatePod();
        updatedMountPod.thenAccept(pod -> {
            pod.setStatus(new PodStatusBuilder()
                .withPhase(PodPhase.RUNNING.getPhase())
                .build());
            mockGetPodByName(pod);
        });
        mockDeleteResource(PERSISTENT_VOLUME_CLAIM_PATH, volumeClaimName, () -> {}, 200);
        mockDeleteResource(PERSISTENT_VOLUME_PATH, volumeName, () -> {}, 200);
        mockUnmountCall(allocatedVm.podName(), dynamicMount.mountPath());
        var unmountOp = unmountDisk(vm.vmId(), dynamicMount.id());

        waitOpSuccess(unmountOp);

        var updatedVm = vmDao.get(vm.vmId(), null);
        Assert.assertEquals(getName(updatedMountPod.get()), updatedVm.instanceProperties().mountPodName());
    }

    private void mockUnmountCall(String podName, String mountPath) {
        var escapedPath = mountPath.replace("/", "%2F");
        kubernetesServer.expect().get()
            .withPath("/api/v1/namespaces/default/pods/" + podName + "/exec?" +
                "command=umount&command=" + escapedPath + "&container=workload&stdout=true&stderr=true")
            .andUpgradeToWebSocket().open(new OutputStreamMessage("hello!")).done()
            .once();
    }

    private static String getClusterId(Vm vm) {
        return vm.allocateState().allocatorMeta().get(KuberVmAllocator.CLUSTER_ID_KEY);
    }

    private LongRunning.Operation mountDisk(String vmId, String mountPath, String diskId, int sizeGb) {
        return authorizedAllocatorBlockingStub.mount(VmAllocatorApi.MountRequest.newBuilder()
            .setVmId(vmId)
            .setMountPath(mountPath)
            .setDiskVolume(VolumeApi.DiskVolumeType.newBuilder()
                .setDiskId(diskId)
                .setSizeGb(sizeGb)
                .build())
            .build());
    }

    private LongRunning.Operation unmountDisk(String vmId, String mountId) {
        return authorizedAllocatorBlockingStub.unmount(VmAllocatorApi.UnmountRequest.newBuilder()
            .setVmId(vmId)
            .setMountId(mountId)
            .build());
    }

    private AllocatedVm allocateWithMountPod(String sessionId) throws Exception {
        var vmPodFuture = mockCreatePod();
        var mountPodFuture = mockCreatePod();
        var operation = authorizedAllocatorBlockingStub.allocate(
            VmAllocatorApi.AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .setClusterType(VmAllocatorApi.AllocateRequest.ClusterType.USER)
                .addWorkload(VmAllocatorApi.AllocateRequest.Workload.newBuilder()
                    .setName("workload")
                    .build())
                .build());
        var allocateMetadata = operation.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);
        var clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        var vmPod = vmPodFuture.get();
        mockGetPodByName(getVmPodName(allocateMetadata.getVmId()));
        var mountPod = getName(mountPodFuture.get());
        Assert.assertTrue(mountPod.startsWith(KuberMountHolderManager.MOUNT_HOLDER_POD_NAME_PREFIX));
        registerVm(allocateMetadata.getVmId(), clusterId);

        waitOpSuccess(operation);

        return new AllocatedVm(allocateMetadata.getVmId(), getName(vmPod), operation.getId());
    }
}
