package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.dao.DynamicMountDao;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator;
import ai.lzy.allocator.model.*;
import ai.lzy.allocator.test.http.MockResponses;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VolumeApi;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.util.Durations;
import io.fabric8.kubernetes.api.model.PersistentVolume;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeStatus;
import io.fabric8.kubernetes.api.model.PersistentVolumeStatusBuilder;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static ai.lzy.allocator.test.http.RequestMatchers.containsPath;
import static ai.lzy.allocator.test.http.RequestMatchers.method;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.requireNonNull;

public class AllocatorServiceMountsTest extends AllocatorApiTestBase {

    public static final String WORKER_MOUNT_POINT = "/mnt/worker";
    public static final String HOST_MOUNT_POINT = "/mnt/host";
    private VmDao vmDao;
    private DynamicMountDao dynamicMountDao;
    private OperationDao operationDao;

    @Before
    public void before() throws IOException {
        vmDao = allocatorContext.getBean(VmDao.class);
        dynamicMountDao = allocatorContext.getBean(DynamicMountDao.class);
        operationDao = allocatorContext.getBean(OperationDao.class);
    }

    @Override
    protected Map<String, Object> allocatorConfigOverrides() {
        return Map.of(
            "allocator.allocation-timeout", "30s",
            "allocator.heartbeat-timeout", "60m",
            "allocator.gc.initial-delay", "60m",
            "allocator.mount-timeout", "30s",
            "allocator.mount.enabled", "true",
            "allocator.mount.pod-image", "ubuntu",
            "allocator.mount.worker-mount-point", WORKER_MOUNT_POINT,
            "allocator.mount.host-mount-point", HOST_MOUNT_POINT
        );
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
        var vm = allocateVm(sessionId);
        vmDao.delete(vm.vmId(), new Vm.DeletingState(vm.allocationOpId(), "foo", "bar"), null);
        var exception = Assert.assertThrows(StatusRuntimeException.class,
            () -> mountDisk(vm.vmId(), "/foo", "disk-42", 42));
        Assert.assertEquals(Status.FAILED_PRECONDITION.getCode(), exception.getStatus().getCode());
        Assert.assertTrue(exception.getMessage().contains("Cannot mount volume to deleting vm"));
    }

    @Test
    public void mountDuplicateMount() throws Exception {
        var sessionId = createSession(Durations.ZERO);
        var vm = allocateVm(sessionId);

        var mountPath = WORKER_MOUNT_POINT + "/foo";
        var bindPath = "/foo";
        var dynamicMount = DynamicMount.createNew(vm.vmId(), "foo", "disk-disk-42",
            mountPath, bindPath, null, new VolumeRequest(UUID.randomUUID().toString(),
                new DiskVolumeDescription(UUID.randomUUID().toString(), "disk-42", 42,
                    Volume.AccessMode.READ_WRITE_ONCE, null, null)), vm.allocationOpId(),
            "allocator");
        dynamicMountDao.create(dynamicMount, null);
        var exception = Assert.assertThrows(StatusRuntimeException.class,
            () -> mountDisk(vm.vmId(), "/foo", "disk-42", 42));
        Assert.assertEquals(Status.ALREADY_EXISTS.getCode(), exception.getStatus().getCode());
        Assert.assertTrue(exception.getMessage().contains("Mount with path /mnt/worker/foo already exists"));
        Assert.assertTrue(exception.getMessage().contains("Mount with bind path /foo already exists"));
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
        var vm = allocateVm(sessionId);
        var mount1 = DynamicMount.createNew(vm.vmId(), "foo", "bar", "/baz", "/bazz",
            null, new VolumeRequest("disk-1", new DiskVolumeDescription("disk-1", "1", 42,
                Volume.AccessMode.READ_WRITE_ONCE, null, null)),
            vm.allocationOpId(), "allocator");
        var mount2 = DynamicMount.createNew(vm.vmId(), "foo", "qux", "/quux", "/hoxo",
            null, new VolumeRequest("disk-1", new DiskVolumeDescription("disk-1", "1", 42,
                Volume.AccessMode.READ_ONLY_MANY,
                DiskVolumeDescription.StorageClass.SSD,
                DiskVolumeDescription.FsType.BTRFS)),
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
        Assert.assertEquals(mount1.bindPath(), fetchedMount1.getBindPath());
        Assert.assertEquals(mount1.state().name(), fetchedMount1.getState());
        Assert.assertEquals(mount1.mountOperationId(), fetchedMount1.getMountOperationId());
        Assert.assertFalse(fetchedMount1.hasUnmountOperationId());
        Assert.assertFalse(fetchedMount1.hasVolumeName());
        Assert.assertFalse(fetchedMount1.hasVolumeClaimName());
        var diskVolumeDescription = (DiskVolumeDescription) mount1.volumeRequest().volumeDescription();
        var diskVolume = fetchedMount1.getVolumeRequest().getDiskVolume();
        Assert.assertEquals(diskVolumeDescription.diskId(), diskVolume.getDiskId());
        Assert.assertEquals(diskVolumeDescription.sizeGb(), diskVolume.getSizeGb());
        Assert.assertEquals(diskVolumeDescription.accessMode().toString(), diskVolume.getAccessMode().toString());
        Assert.assertEquals(VolumeApi.DiskVolumeType.StorageClass.STORAGE_CLASS_UNSPECIFIED,
            diskVolume.getStorageClass());

        var fetchedMount2 = fetchedMountsById.get(mount2.id());
        Assert.assertNotNull(fetchedMount2);
        Assert.assertEquals(mount2.id(), fetchedMount2.getId());
        Assert.assertEquals(mount2.vmId(), fetchedMount2.getVmId());
        Assert.assertEquals(mount2.mountPath(), fetchedMount2.getMountPath());
        Assert.assertEquals(mount2.bindPath(), fetchedMount2.getBindPath());
        Assert.assertEquals(mount2.state().name(), fetchedMount2.getState());
        Assert.assertEquals(mount2.mountOperationId(), fetchedMount2.getMountOperationId());
        Assert.assertFalse(fetchedMount2.hasUnmountOperationId());
        Assert.assertFalse(fetchedMount2.hasVolumeName());
        Assert.assertFalse(fetchedMount2.hasVolumeClaimName());
        diskVolumeDescription = (DiskVolumeDescription) mount2.volumeRequest().volumeDescription();
        var diskVolume2 = fetchedMount2.getVolumeRequest().getDiskVolume();
        Assert.assertEquals(diskVolumeDescription.diskId(), diskVolume2.getDiskId());
        Assert.assertEquals(diskVolumeDescription.sizeGb(), diskVolume2.getSizeGb());
        Assert.assertEquals(diskVolumeDescription.accessMode().toString(), diskVolume2.getAccessMode().toString());
        Assert.assertEquals(VolumeApi.DiskVolumeType.StorageClass.SSD, diskVolume2.getStorageClass());
    }

    @Test
    public void mountTest() throws Exception {
        var sessionId = createSession(Durations.fromDays(10));
        var allocatedVm = allocateVm(sessionId);
        var vm = vmDao.get(allocatedVm.vmId(), null);
        Assert.assertNotNull(vm);

        createReadyMount(vm, allocatedVm.allocationOpId());

        var pv = awaitResourceCreate(PersistentVolume.class, PERSISTENT_VOLUME_PATH);
        var pvc = awaitResourceCreate(PersistentVolumeClaim.class, PERSISTENT_VOLUME_CLAIM_PATH);
        var updatedMountPod = mockCreatePod();
        updatedMountPod.thenAccept(pod -> {
            pod.setStatus(new PodStatusBuilder()
                .withPhase(PodPhase.RUNNING.getPhase())
                .build());
            mockGetPod(pod);
            mockGetPodByName(allocatedVm.podName());
        });
        mockDeletePods(HTTP_OK);
        var mountOp = mountDisk(vm.vmId(), "/foo", "disk-42", 1);

        waitOpSuccess(mountOp);
        var mountMetadata = mountOp.getMetadata().unpack(VmAllocatorApi.MountMetadata.class);
        Assert.assertTrue(mountMetadata.hasMount());
        var dynamicMount = mountMetadata.getMount();
        Assert.assertEquals(vm.vmId(), dynamicMount.getVmId());
        Assert.assertEquals(WORKER_MOUNT_POINT + "/foo", dynamicMount.getMountPath());
        Assert.assertEquals("/foo", dynamicMount.getBindPath());
        Assert.assertEquals("disk-42", dynamicMount.getVolumeRequest().getDiskVolume().getDiskId());
        Assert.assertEquals(1, dynamicMount.getVolumeRequest().getDiskVolume().getSizeGb());
        Assert.assertEquals(VolumeApi.DiskVolumeType.AccessMode.READ_WRITE_ONCE,
            dynamicMount.getVolumeRequest().getDiskVolume().getAccessMode());
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
        Assert.assertEquals("/foo", updatedMount.bindPath());
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
        var mountPod = updatedMountPod.get();
        var mountPodName = getName(mountPod);
        Assert.assertEquals(mountPodName, updatedVm.instanceProperties().mountPodName());
        Assert.assertTrue(mountPodName.endsWith("0"));
        Assert.assertEquals(1, updatedVm.instanceProperties().nextMountPodId());

        Assert.assertEquals(3, mountPod.getSpec().getVolumes().size());
        Assert.assertEquals(3, mountPod.getSpec().getContainers().get(0).getVolumeMounts().size());
        Assert.assertEquals("", pv.get().getSpec().getStorageClassName());
        Assert.assertEquals("", pvc.get().getSpec().getStorageClassName());
    }

    @Test
    public void mountReadOnlySsdDiskTest() throws Exception {
        var sessionId = createSession(Durations.fromDays(10));
        var allocatedVm = allocateVm(sessionId);
        var vm = vmDao.get(allocatedVm.vmId(), null);
        Assert.assertNotNull(vm);

        var pv = awaitResourceCreate(PersistentVolume.class, PERSISTENT_VOLUME_PATH);
        var pvc = awaitResourceCreate(PersistentVolumeClaim.class, PERSISTENT_VOLUME_CLAIM_PATH);
        var updatedMountPod = mockCreatePod();
        updatedMountPod.thenAccept(pod -> {
            pod.setStatus(new PodStatusBuilder()
                .withPhase(PodPhase.RUNNING.getPhase())
                .build());
            mockGetPod(pod);
            mockGetPodByName(allocatedVm.podName());
        });
        mockDeletePods(HTTP_OK);
        var mountOp = mountDisk(vm.vmId(), "/foo", "disk-42", 1,
            VolumeApi.DiskVolumeType.AccessMode.READ_ONLY_MANY, VolumeApi.DiskVolumeType.StorageClass.SSD);

        waitOpSuccess(mountOp);
        var mountMetadata = mountOp.getMetadata().unpack(VmAllocatorApi.MountMetadata.class);
        Assert.assertTrue(mountMetadata.hasMount());
        var dynamicMount = mountMetadata.getMount();
        Assert.assertEquals(VolumeApi.DiskVolumeType.AccessMode.READ_ONLY_MANY,
            dynamicMount.getVolumeRequest().getDiskVolume().getAccessMode());

        var persistentVolume = pv.get();
        Assert.assertTrue(persistentVolume.getSpec().getCsi().getReadOnly());
        Assert.assertEquals(1, persistentVolume.getSpec().getAccessModes().size());
        Assert.assertEquals(Volume.AccessMode.READ_ONLY_MANY.asString(),
            persistentVolume.getSpec().getAccessModes().get(0));

        var persistentVolumeClaim = pvc.get();
        Assert.assertEquals(1, persistentVolumeClaim.getSpec().getAccessModes().size());
        Assert.assertEquals(Volume.AccessMode.READ_ONLY_MANY.asString(),
            persistentVolumeClaim.getSpec().getAccessModes().get(0));
        Assert.assertEquals(getName(persistentVolume), persistentVolumeClaim.getSpec().getVolumeName());
        Assert.assertEquals("yc-network-ssd", persistentVolume.getSpec().getStorageClassName());
        Assert.assertEquals("yc-network-ssd", persistentVolumeClaim.getSpec().getStorageClassName());
    }

    @Test
    public void mountShouldNotFailOnExistingVolumes() throws Exception {
        var sessionId = createSession(Durations.fromDays(10));
        var allocatedVm = allocateVm(sessionId);
        var vm = vmDao.get(allocatedVm.vmId(), null);

        awaitResourceCreate(PersistentVolume.class, PERSISTENT_VOLUME_PATH, HTTP_CONFLICT)
            .thenAccept(pv -> {
                pv.setStatus(new PersistentVolumeStatus("Running", PersistentVolumePhase.BOUND.getPhase(), null));
                mockGetPv(pv);
            });
        awaitResourceCreate(PersistentVolumeClaim.class, PERSISTENT_VOLUME_CLAIM_PATH, HTTP_CONFLICT);
        var updatedMountPod = mockCreatePod();
        updatedMountPod.thenAccept(pod -> {
            pod.setStatus(new PodStatusBuilder()
                .withPhase(PodPhase.RUNNING.getPhase())
                .build());
            mockGetPod(pod);
            mockGetPodByName(allocatedVm.podName());
        });
        mockDeletePods(HTTP_OK);
        var mountOp = mountDisk(vm.vmId(), "/foo", "disk-42", 1);
        waitOpSuccess(mountOp);
    }

    @Test
    public void mountShouldFailOnDeletedVmPod() throws Exception {
        var sessionId = createSession(Durations.fromDays(10));
        var allocatedVm = allocateVm(sessionId);
        var vm = vmDao.get(allocatedVm.vmId(), null);

        awaitResourceCreate(PersistentVolume.class, PERSISTENT_VOLUME_PATH);
        awaitResourceCreate(PersistentVolumeClaim.class, PERSISTENT_VOLUME_CLAIM_PATH);
        var updatedMountPod = mockCreatePod();
        updatedMountPod.thenAccept(pod -> {
            pod.setStatus(new PodStatusBuilder()
                .withPhase(PodPhase.RUNNING.getPhase())
                .build());
            mockGetPod(pod);
            mockGetPodNotFound(allocatedVm.podName());
        });
        mockDeletePods(HTTP_OK);
        var mountOp = mountDisk(vm.vmId(), "/foo", "disk-42", 1);
        waitOpError(mountOp, Status.CANCELLED);
    }

    @Test
    public void mountShouldWaitTerminatingExistingVolumesAndRecreate() throws Exception {
        var sessionId = createSession(Durations.fromDays(10));
        var allocatedVm = allocateVm(sessionId);
        var vm = vmDao.get(allocatedVm.vmId(), null);

        awaitResourceCreate(PersistentVolume.class, PERSISTENT_VOLUME_PATH, HTTP_CONFLICT)
            .thenAccept(pv -> {
                pv.setStatus(new PersistentVolumeStatusBuilder()
                    .withMessage("Terminating")
                    .withPhase(PersistentVolumePhase.RELEASED.getPhase())
                    .build());
                mockGetPv(pv); // PV exists but status is RELEASED

                mockGetPvNotFound(getName(pv)); // NOT_FOUND on next call

                mockCreatePv(); // successfully (re)create PV
            });
        awaitResourceCreate(PersistentVolumeClaim.class, PERSISTENT_VOLUME_CLAIM_PATH, HTTP_CONFLICT);

        var updatedMountPod = mockCreatePod();
        updatedMountPod.thenAccept(pod -> {
            pod.setStatus(new PodStatusBuilder()
                .withPhase(PodPhase.RUNNING.getPhase())
                .build());
            mockGetPod(pod);
            mockGetPodByName(allocatedVm.podName());
        });
        mockDeletePods(HTTP_OK);
        var mountOp = mountDisk(vm.vmId(), "/foo", "disk-42", 1);
        waitOpSuccess(mountOp);
    }

    @Test
    public void unmountNotFound() {
        var exception = Assert.assertThrows(StatusRuntimeException.class, () -> unmountDisk("foo"));
        Assert.assertEquals(Status.NOT_FOUND.getCode(), exception.getStatus().getCode());
        Assert.assertTrue(exception.getMessage().contains("Mount with id foo not found"));
    }

    @Test
    public void unmountValidation() throws Exception {
        var operation = Operation.create("foo", "bar", null, null);
        operationDao.create(operation, null);

        var mountPath = WORKER_MOUNT_POINT + "/foo";
        var bindPath = "/foo";
        var dynamicMount = DynamicMount.createNew(null, "foo", "disk-42",
            mountPath, bindPath, null, new VolumeRequest(UUID.randomUUID().toString(),
                new DiskVolumeDescription(UUID.randomUUID().toString(), "disk-42", 42,
                    Volume.AccessMode.READ_WRITE_ONCE, null, null)),
            operation.id(), "allocator-0");
        dynamicMountDao.create(dynamicMount, null);
        var volumeClaimName = "claim-42";
        var volumeName = "volume-42";
        var update = DynamicMount.Update.builder()
            .volumeClaimName(volumeClaimName)
            .volumeName(volumeName)
            .state(DynamicMount.State.DELETING)
            .unmountOperationId(operation.id())
            .build();
        dynamicMountDao.update(dynamicMount.id(), update, null);
        var exception = Assert.assertThrows(StatusRuntimeException.class,
            () -> unmountDisk(dynamicMount.id()));
        Assert.assertEquals(Status.FAILED_PRECONDITION.getCode(), exception.getStatus().getCode());
        Assert.assertTrue(exception.getMessage()
            .contains("Mount with id %s is in wrong state: %s. Expected state: READY".formatted(dynamicMount.id(),
                DynamicMount.State.DELETING)));
        Assert.assertTrue(exception.getMessage()
            .contains("Mount with id %s is already unmounting".formatted(dynamicMount.id())));
    }

    @Test
    public void unmountTest() throws Exception {
        var sessionId = createSession(Durations.fromDays(10));
        var allocatedVm = allocateVm(sessionId);
        var vm = vmDao.get(allocatedVm.vmId(), null);
        Assert.assertNotNull(vm);

        var mountPath = WORKER_MOUNT_POINT + "/foo";
        var bindPath = "/foo";
        var dynamicMount = DynamicMount.createNew(allocatedVm.vmId(), getClusterId(vm), "disk-42",
            mountPath, bindPath, null, new VolumeRequest(UUID.randomUUID().toString(),
                new DiskVolumeDescription(UUID.randomUUID().toString(), "disk-42", 42,
                    Volume.AccessMode.READ_WRITE_ONCE, null, null)),
            allocatedVm.allocationOpId(), "allocator-0");
        dynamicMountDao.create(dynamicMount, null);
        var volumeClaimName = "claim-42";
        var volumeName = "volume-42";
        var update = DynamicMount.Update.builder()
            .volumeClaimName(volumeClaimName)
            .volumeName(volumeName)
            .state(DynamicMount.State.READY)
            .mounted(true)
            .build();
        dynamicMountDao.update(dynamicMount.id(), update, null);

        mockDeletePods(HTTP_OK);
        mockDeleteResource(PERSISTENT_VOLUME_CLAIM_PATH, volumeClaimName, () -> {}, HTTP_OK);
        mockDeleteResource(PERSISTENT_VOLUME_PATH, volumeName, () -> {}, HTTP_OK);
        mockUnmountCall();
        var unmountOp = unmountDisk(dynamicMount.id());

        waitOpSuccess(unmountOp);

        var updatedVm = vmDao.get(vm.vmId(), null);
        Assert.assertNull(updatedVm.instanceProperties().mountPodName());
        var mount = dynamicMountDao.get(dynamicMount.id(), false, null);
        Assert.assertNull(mount);
    }

    @Test
    public void unmountShouldNotDeleteUsedVolume() throws Exception {
        var sessionId = createSession(Durations.fromDays(10));
        var allocatedVm = allocateVm(sessionId);
        var vm = vmDao.get(allocatedVm.vmId(), null);
        Assert.assertNotNull(vm);

        var mountPath = WORKER_MOUNT_POINT + "/foo";
        var bindPath = "/foo";
        var dynamicMount = DynamicMount.createNew(allocatedVm.vmId(), getClusterId(vm), "disk-42",
            mountPath, bindPath, null, new VolumeRequest(UUID.randomUUID().toString(),
                new DiskVolumeDescription(UUID.randomUUID().toString(), "disk-42", 42,
                    Volume.AccessMode.READ_WRITE_ONCE, null, null)),
            allocatedVm.allocationOpId(), "allocator-0");
        var anotherMount = createReadyMount(vm, allocatedVm.allocationOpId());

        dynamicMountDao.create(dynamicMount, null);
        var volumeClaimName = "claim-42";
        var volumeName = "volume-42";
        var update = DynamicMount.Update.builder()
            .volumeClaimName(volumeClaimName)
            .volumeName(volumeName)
            .state(DynamicMount.State.READY)
            .mounted(true)
            .build();
        dynamicMountDao.update(dynamicMount.id(), update, null);
        dynamicMountDao.update(anotherMount.id(), update, null);

        var updatedMountPod = mockCreatePod();
        updatedMountPod.thenAccept(pod -> {
            pod.setStatus(new PodStatusBuilder()
                .withPhase(PodPhase.RUNNING.getPhase())
                .build());
            mockGetPod(pod);
        });
        mockDeletePods(HTTP_OK);
        var volumeDeleted = new AtomicBoolean(false);
        var volumeClaimDeleted = new AtomicBoolean(false);
        mockDeleteResource(PERSISTENT_VOLUME_CLAIM_PATH, volumeClaimName, () -> volumeClaimDeleted.set(true), 403);
        mockDeleteResource(PERSISTENT_VOLUME_PATH, volumeName, () -> volumeDeleted.set(true), 403);
        mockUnmountCall();
        var unmountOp = unmountDisk(dynamicMount.id());

        waitOpSuccess(unmountOp);

        Assert.assertFalse(volumeDeleted.get());
        Assert.assertFalse(volumeClaimDeleted.get());
        var updatedVm = vmDao.get(vm.vmId(), null);
        var mountPod = updatedMountPod.get();
        Assert.assertEquals(getName(mountPod), updatedVm.instanceProperties().mountPodName());
        var vmMounts = dynamicMountDao.getByVm(vm.vmId(), null);
        Assert.assertEquals(1, vmMounts.size());
        Assert.assertEquals(anotherMount.id(), vmMounts.get(0).id());

        Assert.assertEquals(2, mountPod.getSpec().getVolumes().size());
        Assert.assertEquals(2, mountPod.getSpec().getContainers().get(0).getVolumeMounts().size());
    }

    @Test
    public void vmShouldNotAllocateMountPodInSystemCluster() throws Exception {
        var sessionId = createSession(Durations.fromDays(10));
        var allocatedVm = allocateSystemVm(sessionId);
        var vm = vmDao.get(allocatedVm.vmId(), null);
        Assert.assertNotNull(vm);
        var mountPodName = vm.instanceProperties().mountPodName();
        Assert.assertNull(mountPodName);
    }

    @Test
    public void unmountShouldRecreateMountPodWithNotMountedDisk() throws Exception {
        var sessionId = createSession(Durations.fromDays(10));
        var allocatedVm = allocateVm(sessionId);
        var vm = vmDao.get(allocatedVm.vmId(), null);
        Assert.assertNotNull(vm);
        var dynamicMount = createReadyMount(vm, allocatedVm.allocationOpId());
        var update = DynamicMount.Update.builder()
            .mounted(false)
            .build();
        var mountPodName = "foobar-42";
        vmDao.setMountPodAndIncrementNextId(vm.vmId(), mountPodName, null);
        dynamicMount = dynamicMountDao.update(dynamicMount.id(), update, null);
        Assert.assertNotNull(dynamicMount);

        var mountPodCreated = new AtomicBoolean(false);
        var otherPodsDeleted = new AtomicBoolean(false);
        var volumeDeleted = new AtomicBoolean(false);
        var volumeClaimDeleted = new AtomicBoolean(false);
        mockCreatePod(pod -> mountPodCreated.set(true));
        mockUnmountCall();
        mockDeletePods(HTTP_OK, () -> otherPodsDeleted.set(true));
        mockDeleteResource(PERSISTENT_VOLUME_CLAIM_PATH, dynamicMount.volumeClaimName(),
            () -> volumeClaimDeleted.set(true), HTTP_OK);
        mockDeleteResource(PERSISTENT_VOLUME_PATH, dynamicMount.volumeName(), () -> volumeDeleted.set(true),
            HTTP_OK);

        var operation = unmountDisk(dynamicMount.id());
        waitOpSuccess(operation);

        Assert.assertFalse(mountPodCreated.get());
        Assert.assertTrue(otherPodsDeleted.get());
        Assert.assertTrue(volumeDeleted.get());
        Assert.assertTrue(volumeClaimDeleted.get());
        vm = vmDao.get(vm.vmId(), null);
        Assert.assertNotNull(vm);
        Assert.assertEquals(mountPodName, vm.instanceProperties().mountPodName());
        dynamicMount = dynamicMountDao.get(dynamicMount.id(), false, null);
        Assert.assertNull(dynamicMount);
    }

    @NotNull
    private DynamicMount createReadyMount(Vm vm, String operationId) throws SQLException {
        return createReadyMount(vm, operationId, null);
    }

    @NotNull
    private DynamicMount createReadyMount(Vm vm, String operationId, @Nullable Volume.AccessMode readWriteOnce)
        throws SQLException
    {
        var id = UUID.randomUUID().toString();
        var mountPath = WORKER_MOUNT_POINT + "/" + id;
        var bindPath = "/" + id;
        var anotherMount = DynamicMount.createNew(vm.vmId(), getClusterId(vm), id, mountPath, bindPath,
            null, new VolumeRequest(id, new DiskVolumeDescription(id, "disk-" + id, 42, readWriteOnce, null, null)),
            operationId, "allocator");
        dynamicMountDao.create(anotherMount, null);
        var update = DynamicMount.Update.builder()
            .state(DynamicMount.State.READY)
            .volumeClaimName("claim-volume-" + id)
            .volumeName("volume-" + id)
            .mounted(true)
            .build();
        return dynamicMountDao.update(anotherMount.id(), update, null);
    }

    private void mockUnmountCall() {
        mockRequestDispatcher.addHandlerOneTime(containsPath("/exec").and(method("GET")),
            request -> MockResponses.websocketUpgradeSendAndClose("eval exec"));
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

    private LongRunning.Operation mountDisk(String vmId, String mountPath, String diskId, int sizeGb,
                                            VolumeApi.DiskVolumeType.AccessMode accessMode,
                                            VolumeApi.DiskVolumeType.StorageClass storageClass)
    {
        return authorizedAllocatorBlockingStub.mount(VmAllocatorApi.MountRequest.newBuilder()
            .setVmId(vmId)
            .setMountPath(mountPath)
            .setDiskVolume(VolumeApi.DiskVolumeType.newBuilder()
                .setDiskId(diskId)
                .setSizeGb(sizeGb)
                .setAccessMode(accessMode)
                .setStorageClass(storageClass)
                .build())
            .build());
    }

    private LongRunning.Operation unmountDisk(String mountId) {
        return authorizedAllocatorBlockingStub.unmount(VmAllocatorApi.UnmountRequest.newBuilder()
            .setMountId(mountId)
            .build());
    }

    private AllocatedVm allocateVm(String sessionId) throws Exception {
        var vmPodFuture = mockCreatePod();
        vmPodFuture.thenAccept(pod -> mockGetPodByName(getName(pod)));
        vmPodFuture.thenAccept(this::mockExecInPod);
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
        registerVm(allocateMetadata.getVmId(), clusterId);

        waitOpSuccess(operation);

        return new AllocatedVm(allocateMetadata.getVmId(), getName(vmPod), operation.getId());
    }

    private AllocatedVm allocateSystemVm(String sessionId) throws Exception {
        var vmPodFuture = mockCreatePod();
        vmPodFuture.thenAccept(pod -> mockGetPodByName(getName(pod)));
        var operation = authorizedAllocatorBlockingStub.allocate(
            VmAllocatorApi.AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .setClusterType(VmAllocatorApi.AllocateRequest.ClusterType.SYSTEM)
                .addWorkload(VmAllocatorApi.AllocateRequest.Workload.newBuilder()
                    .setName("workload")
                    .build())
                .build());
        var allocateMetadata = operation.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);
        var clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE,
            ClusterRegistry.ClusterType.System)).clusterId();
        var vmPod = vmPodFuture.get();
        registerVm(allocateMetadata.getVmId(), clusterId);

        waitOpSuccess(operation);

        return new AllocatedVm(allocateMetadata.getVmId(), getName(vmPod), operation.getId());
    }
}
