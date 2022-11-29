package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.*;
import ai.lzy.v1.VmAllocatorApi.AllocateRequest;
import ai.lzy.v1.VmAllocatorApi.AllocateResponse;
import ai.lzy.v1.VmAllocatorApi.DeleteSessionRequest;
import ai.lzy.v1.VmAllocatorApi.FreeRequest;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunning.Operation;
import com.google.protobuf.util.Durations;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator.VM_POD_NAME_PREFIX;
import static ai.lzy.allocator.model.Volume.AccessMode.READ_WRITE_ONCE;
import static ai.lzy.allocator.test.Utils.waitOperation;
import static ai.lzy.allocator.volume.KuberVolumeManager.KUBER_GB_NAME;
import static ai.lzy.allocator.volume.KuberVolumeManager.VOLUME_CAPACITY_STORAGE_KEY;
import static ai.lzy.allocator.volume.KuberVolumeManager.YCLOUD_DISK_DRIVER;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static java.util.Objects.requireNonNull;

public class AllocatorServiceTest extends AllocatorApiTestBase {

    private static final int TIMEOUT_SEC = 300;

    private static final String ZONE = "test-zone";


    @Before
    public void before() throws IOException {
        super.setUp();
    }

    @After
    public void after() {
        super.tearDown();
    }

    @Test
    public void testUnauthenticated() {
        try {
            //noinspection ResultOfMethodCallIgnored
            unauthorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
        try {
            //noinspection ResultOfMethodCallIgnored
            unauthorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testPermissionDenied() {
        final AllocatorGrpc.AllocatorBlockingStub invalidAuthorizedAllocatorBlockingStub =
            unauthorizedAllocatorBlockingStub.withInterceptors(
                ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                    JwtUtils.invalidCredentials("user", "GITHUB")::token));
        try {
            //noinspection ResultOfMethodCallIgnored
            invalidAuthorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
        try {
            //noinspection ResultOfMethodCallIgnored
            invalidAuthorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void allocateKuberErrorWhileCreate() throws Exception {
        //simulate kuber api error on pod creation
        kubernetesServer.getKubernetesMockServer().clearExpectations();
        kubernetesServer.expect().post().withPath(POD_PATH)
            .andReturn(HttpURLConnection.HTTP_INTERNAL_ERROR, new PodListBuilder().build())
            .once();

        var sessionId = createSession(Durations.fromSeconds(100));

        final Operation operation = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadata =
            operation.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = KuberVmAllocator.VM_POD_NAME_PREFIX + allocateMetadata.getVmId();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        waitOpError(operation, Status.INTERNAL);
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void allocateServantTimeout() throws Exception {
        var sessionId = createSession(Durations.fromSeconds(100));

        final var future = awaitAllocationRequest();

        final Operation operation = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                .build());

        final String podName = future.get();
        mockGetPod(podName);
        
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        waitOpError(operation, Status.DEADLINE_EXCEEDED);
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void allocateInvalidPool() {
        var sessionId = createSession(Durations.fromSeconds(100));

        waitOpError(
            authorizedAllocatorBlockingStub.allocate(
                AllocateRequest.newBuilder()
                    .setSessionId(sessionId)
                    .setPoolLabel("LABEL")
                    .setZone(ZONE)
                    .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                    .build()),
            Status.INVALID_ARGUMENT);
    }

    @Test
    public void allocateInvalidSession() {
        try {
            waitOperation(
                operationServiceApiBlockingStub,
                authorizedAllocatorBlockingStub.allocate(
                    AllocateRequest.newBuilder()
                        .setSessionId(UUID.randomUUID().toString())
                        .setPoolLabel("S")
                        .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                        .build()),
                TIMEOUT_SEC
            );
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    private record AllocatedVm(
        String vmId,
        String podName,
        Subject iamSubj
    ) {}

    private AllocatedVm allocateVm(String sessionId, @Nullable String idempotencyKey) throws Exception {
        final var future = awaitAllocationRequest();

        var stub = authorizedAllocatorBlockingStub;
        if (idempotencyKey != null) {
            stub = withIdempotencyKey(stub, idempotencyKey);
        }

        var allocOp = stub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                .build());
        Assert.assertFalse(allocOp.getDone());
        var vmId = allocOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();

        final String podName = future.get();
        mockGetPod(podName);

        String clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        registerVm(vmId, clusterId);

        allocOp = waitOpSuccess(allocOp);
        Assert.assertEquals(vmId, allocOp.getResponse().unpack(AllocateResponse.class).getVmId());

        var vmSubj = super.getSubject(AuthProvider.INTERNAL, vmId, SubjectType.VM);
        Assert.assertNotNull(vmSubj);

        return new AllocatedVm(vmId, podName, vmSubj);
    }

    private AllocatedVm allocateAndFreeVm(String sessionId, Consumer<AllocatedVm> beforeFree) throws Exception {
        var vm = allocateVm(sessionId, null);
        beforeFree.accept(vm);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(vm.vmId).build());

        return vm;
    }

    @Test
    public void allocateFreeSuccess() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        var vm1 = allocateAndFreeVm(sessionId,
            vm -> mockDeletePod(vm.podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK));

        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        var vmSubj = super.getSubject(AuthProvider.INTERNAL, vm1.vmId, SubjectType.VM);
        Assert.assertNull(vmSubj);
    }

    @Test
    public void idempotentAllocate() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        var vm1 = allocateVm(sessionId, "key-1");

        var allocOp = withIdempotencyKey(authorizedAllocatorBlockingStub, "key-1").allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                .build());
        Assert.assertTrue(allocOp.getDone());

        var vmId = allocOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();
        Assert.assertEquals(vm1.vmId, vmId);

        vmId = allocOp.getResponse().unpack(VmAllocatorApi.AllocateResponse.class).getVmId();
        Assert.assertEquals(vm1.vmId, vmId);
    }

    @Test
    public void idempotentConcurrentAllocate() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        final int N = 10;
        final var readyLatch = new CountDownLatch(N);
        final var doneLatch = new CountDownLatch(N);
        final var executor = Executors.newFixedThreadPool(N);
        final var failed = new AtomicBoolean(false);
        final var allocatedVmIds = new String[N];

        for (int i = 0; i < N; ++i) {
            final int index = i;
            executor.submit(() -> {
                try {
                    readyLatch.countDown();
                    readyLatch.await();

                    var allocOp = withIdempotencyKey(authorizedAllocatorBlockingStub, "key-1").allocate(
                        AllocateRequest.newBuilder()
                            .setSessionId(sessionId)
                            .setPoolLabel("S")
                            .setZone(ZONE)
                            .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                            .build());

                    var vmId = allocOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();

                    while (vmId.isEmpty()) {
                        LockSupport.parkNanos(Duration.ofMillis(1).toNanos());
                        allocOp = operationServiceApiBlockingStub.get(
                            LongRunning.GetOperationRequest.newBuilder()
                                .setOperationId(allocOp.getId())
                                .build());
                        vmId = allocOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();
                    }

                    allocatedVmIds[index] = vmId;
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                    failed.set(true);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        doneLatch.await();
        executor.shutdown();
        Assert.assertFalse(failed.get());

        Assert.assertTrue(Arrays.stream(allocatedVmIds).allMatch(Objects::nonNull));
        Assert.assertTrue(Arrays.stream(allocatedVmIds).allMatch(vmId -> allocatedVmIds[0].equals(vmId)));
    }

    @Test
    public void eventualFreeAfterFail() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(2);

        var vm1 = allocateAndFreeVm(sessionId, vm ->
            mockDeletePod(vm.podName, () -> {
                kuberRemoveRequestLatch.countDown();
                mockDeletePod(vm.podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);
            },
            HttpURLConnection.HTTP_INTERNAL_ERROR /* fail the first remove request */));

        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        var vmSubj = super.getSubject(AuthProvider.INTERNAL, vm1.vmId, SubjectType.VM);
        Assert.assertNull(vmSubj);
    }

    @Test
    public void allocateFromCache() throws Exception {
        var sessionId = createSession(Durations.fromSeconds(10));

        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);

        var cachedVm = allocateAndFreeVm(sessionId,
            vm -> mockDeletePod(vm.podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK));

        var operationSecond = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                .build());
        var allocateMetadataSecond = operationSecond.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);
        operationSecond = waitOpSuccess(operationSecond);

        final var allocateResponseSecond = operationSecond.getResponse().unpack(AllocateResponse.class);
        Assert.assertEquals(cachedVm.vmId, allocateResponseSecond.getVmId());

        var vmSubj2 = super.getSubject(AuthProvider.INTERNAL, allocateMetadataSecond.getVmId(), SubjectType.VM);
        Assert.assertNotNull(vmSubj2);

        Assert.assertEquals(cachedVm.vmId, allocateMetadataSecond.getVmId());
        Assert.assertEquals(cachedVm.iamSubj, vmSubj2);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(
            FreeRequest.newBuilder().setVmId(allocateMetadataSecond.getVmId()).build());

        Assert.assertEquals(cachedVm.vmId, allocateMetadataSecond.getVmId());
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        var vmSubj = super.getSubject(AuthProvider.INTERNAL, cachedVm.vmId, SubjectType.VM);
        Assert.assertNull(vmSubj);
    }

    @Test
    public void allocateConcurrentFromCache() throws Exception {
        var sessionId = createSession(Durations.fromSeconds(5000));

        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);

        var cachedVm = allocateAndFreeVm(sessionId,
            vm -> mockDeletePod(vm.podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK));

        final int N = 10;
        final var readyLatch = new CountDownLatch(N);
        final var doneLatch = new CountDownLatch(N);
        final var executor = Executors.newFixedThreadPool(N);
        final var failed = new AtomicBoolean(false);
        final var allocatedVmIds = new String[N];

        for (int i = 0; i < N; ++i) {
            final int index = i;
            executor.submit(() -> {
                try {
                    readyLatch.countDown();
                    readyLatch.await();

                    var allocOp = authorizedAllocatorBlockingStub.allocate(
                        AllocateRequest.newBuilder()
                            .setSessionId(sessionId)
                            .setPoolLabel("S")
                            .setZone(ZONE)
                            .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                            .build());
                    allocOp = waitOpSuccess(allocOp);

                    var allocResp = allocOp.getResponse().unpack(AllocateResponse.class);
                    allocatedVmIds[index] = allocResp.getVmId();
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                    failed.set(true);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        doneLatch.await();
        executor.shutdown();
        Assert.assertFalse(failed.get());

        var n = Arrays.stream(allocatedVmIds).filter(cachedVm.vmId::equals).count();
        Assert.assertEquals(1, n);
    }

    @Test
    public void deleteSessionWithActiveVmsAfterRegister() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        var future = awaitAllocationRequest();

        var allocate = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                .build());
        var allocateMetadata = allocate.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        String clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        registerVm(allocateMetadata.getVmId(), clusterId);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.deleteSession(
            DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId)
                .build());

        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        var vmSubj = super.getSubject(AuthProvider.INTERNAL, allocateMetadata.getVmId(), SubjectType.VM);
        Assert.assertNull(vmSubj);
    }

    @Test
    public void deleteSessionWithActiveVmsBeforeRegister() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        var future = awaitAllocationRequest();

        var allocate = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                .build());
        var allocateMetadata = allocate.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.deleteSession(
            DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId)
                .build());

        try {
            //noinspection ResultOfMethodCallIgnored
            privateAllocatorBlockingStub.register(
                VmAllocatorPrivateApi.RegisterRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.FAILED_PRECONDITION.getCode(),
                e.getStatus().getCode());
        }

        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void freeNonexistentVm() {
        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.free(
                FreeRequest.newBuilder().setVmId(UUID.randomUUID().toString()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void repeatedFree() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);

        var vm1 = allocateAndFreeVm(sessionId,
            vm -> mockDeletePod(vm.podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK));

        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(vm1.vmId).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.FAILED_PRECONDITION.getCode(),
                e.getStatus().getCode());
        }
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void repeatedServantRegister() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        var future = awaitAllocationRequest();

        var allocate = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                .build());
        var allocateMetadata = allocate.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        String clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        registerVm(allocateMetadata.getVmId(), clusterId);

        try {
            //noinspection ResultOfMethodCallIgnored
            privateAllocatorBlockingStub.register(
                VmAllocatorPrivateApi.RegisterRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.ALREADY_EXISTS.getCode(),
                e.getStatus().getCode());
        }

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void runWithVolumes() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        var future = awaitAllocationRequest();

        final String volumeName = "volumeName";
        final String mountPath = "/mnt/volume";
        final VolumeApi.Mount volumeMount = VolumeApi.Mount.newBuilder()
            .setVolumeName(volumeName)
            .setMountPath(mountPath)
            .setReadOnly(false)
            .setMountPropagation(VolumeApi.Mount.MountPropagation.NONE)
            .build();

        final DiskApi.DiskSpec diskSpec = DiskApi.DiskSpec.newBuilder()
            .setName("disk-name")
            .setSizeGb(4)
            .setZoneId("ru-central1-a")
            .setType(DiskApi.DiskType.HDD)
            .build();
        Operation createDiskOperation = diskService.createDisk(DiskServiceApi.CreateDiskRequest.newBuilder()
            .setUserId("user-id")
            .setDiskSpec(diskSpec)
            .build());
        createDiskOperation = waitOpSuccess(createDiskOperation);
        Assert.assertTrue(createDiskOperation.hasResponse());
        final DiskServiceApi.CreateDiskResponse createDiskResponse = createDiskOperation.getResponse().unpack(
            DiskServiceApi.CreateDiskResponse.class);
        final DiskApi.Disk disk = createDiskResponse.getDisk();

        final var persistentVolumeFuture = awaitResourceCreate(PersistentVolume.class, PERSISTENT_VOLUME_PATH);
        final var persistentVolumeClaimFuture =
            awaitResourceCreate(PersistentVolumeClaim.class, PERSISTENT_VOLUME_CLAIM_PATH);

        final Operation allocationStarted = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .addWorkload(AllocateRequest.Workload.newBuilder()
                    .setName("workload")
                    .addVolumeMounts(volumeMount)
                    .build())
                .addVolumes(VolumeApi.Volume.newBuilder()
                    .setName(volumeName)
                    .setDiskVolume(VolumeApi.DiskVolumeType.newBuilder()
                        .setDiskId(disk.getDiskId()).build())
                    .build())
                .build());
        var allocateMetadata = allocationStarted.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);

        final PersistentVolume persistentVolume = persistentVolumeFuture.get();
        final PersistentVolumeClaim persistentVolumeClaim = persistentVolumeClaimFuture.get();
        final PersistentVolumeSpec volumeSpec = persistentVolume.getSpec();
        Assert.assertEquals(disk.getDiskId(), volumeSpec.getCsi().getVolumeHandle());
        Assert.assertEquals(YCLOUD_DISK_DRIVER, volumeSpec.getCsi().getDriver());
        final Quantity expectedDiskSize = new Quantity(disk.getSpec().getSizeGb() + KUBER_GB_NAME);
        Assert.assertEquals(
            expectedDiskSize,
            volumeSpec.getCapacity().get("storage")
        );

        Assert.assertEquals(persistentVolume.getMetadata().getName(), persistentVolumeClaim.getSpec().getVolumeName());
        Assert.assertEquals("", persistentVolumeClaim.getSpec().getStorageClassName());
        Assert.assertEquals(READ_WRITE_ONCE.asString(), persistentVolumeClaim.getSpec().getAccessModes().get(0));
        Assert.assertEquals(expectedDiskSize,
            persistentVolumeClaim.getSpec().getResources().getRequests().get(VOLUME_CAPACITY_STORAGE_KEY));

        final CountDownLatch kuberRemoveResourceLatch = new CountDownLatch(3);
        mockDeletePod(podName, kuberRemoveResourceLatch::countDown, HttpURLConnection.HTTP_OK);
        mockDeleteResource(
            PERSISTENT_VOLUME_PATH,
            persistentVolume.getMetadata().getName(),
            kuberRemoveResourceLatch::countDown,
            HttpURLConnection.HTTP_OK
        );
        mockDeleteResource(
            PERSISTENT_VOLUME_CLAIM_PATH,
            persistentVolumeClaim.getMetadata().getName(),
            kuberRemoveResourceLatch::countDown,
            HttpURLConnection.HTTP_OK
        );

        String clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        registerVm(allocateMetadata.getVmId(), clusterId);

        waitOpSuccess(allocationStarted);
        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());
        Assert.assertTrue(kuberRemoveResourceLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        //noinspection ResultOfMethodCallIgnored
        diskService.deleteDisk(DiskServiceApi.DeleteDiskRequest.newBuilder().setDiskId(disk.getDiskId()).build());
    }

    @Test
    public void runWithVolumeButNonExistingDisk() {
        var sessionId = createSession(Durations.ZERO);

        final String volumeName = "volumeName";
        final String mountPath = "/mnt/volume";
        final VolumeApi.Mount volumeMount = VolumeApi.Mount.newBuilder()
            .setVolumeName(volumeName)
            .setMountPath(mountPath)
            .setReadOnly(false)
            .setMountPropagation(VolumeApi.Mount.MountPropagation.NONE)
            .build();

        try {
            Operation allocationStarted = authorizedAllocatorBlockingStub.allocate(
                AllocateRequest.newBuilder()
                    .setSessionId(sessionId)
                    .setPoolLabel("S")
                    .setZone(ZONE)
                    .addWorkload(AllocateRequest.Workload.newBuilder()
                        .setName("workload")
                        .addVolumeMounts(volumeMount)
                        .build())
                    .addVolumes(VolumeApi.Volume.newBuilder()
                        .setName(volumeName)
                        .setDiskVolume(VolumeApi.DiskVolumeType.newBuilder()
                            .setDiskId("unknown-disk-id").build())
                        .build())
                    .build());
            waitOpError(allocationStarted, Status.NOT_FOUND);
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
    }

    private <T> Future<T> awaitResourceCreate(Class<T> resourceType, String resourcePath) {
        final var future = new CompletableFuture<T>();
        kubernetesServer.expect().post()
            .withPath(resourcePath)
            .andReply(HttpURLConnection.HTTP_CREATED, (req) -> {
                final var resource = Serialization.unmarshal(
                    new ByteArrayInputStream(req.getBody().readByteArray()), resourceType, Map.of());
                future.complete(resource);
                return resource;
            })
            .once();
        return future;
    }

    private Future<String> awaitAllocationRequest() {
        final var future = new CompletableFuture<String>();
        kubernetesServer.expect().post()
            .withPath(POD_PATH)
            .andReply(HttpURLConnection.HTTP_CREATED, (req) -> {
                final var pod = Serialization.unmarshal(
                    new ByteArrayInputStream(req.getBody().readByteArray()), Pod.class, Map.of());
                future.complete(pod.getMetadata().getName());
                return pod;
            })
            .once();
        return future;
    }

    private void mockDeleteResource(String resourcePath, String resourceName, Runnable onDelete, int responseCode) {
        kubernetesServer.expect().delete()
            .withPath(resourcePath + "/" + resourceName)
            .andReply(responseCode, (req) -> {
                onDelete.run();
                return new StatusDetails();
            }).once();
    }

    private void mockDeletePod(String podName, Runnable onDelete, int responseCode) {
        mockDeleteResource(POD_PATH, podName, onDelete, responseCode);
        kubernetesServer.expect().delete()
            // "lzy.ai/vm-id"=<VM id>
            .withPath(POD_PATH + "?labelSelector=lzy.ai%2Fvm-id%3D" + podName.substring(VM_POD_NAME_PREFIX.length()))
            .andReply(responseCode, (req) -> {
                onDelete.run();
                return new StatusDetails();
            }).once();
    }


}
