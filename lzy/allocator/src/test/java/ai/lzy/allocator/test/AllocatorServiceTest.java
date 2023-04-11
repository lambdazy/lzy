package ai.lzy.allocator.test;

import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.credentials.OttHelper;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.*;
import ai.lzy.v1.VmAllocatorApi.AllocateMetadata;
import ai.lzy.v1.VmAllocatorApi.AllocateRequest;
import ai.lzy.v1.VmAllocatorApi.AllocateResponse;
import ai.lzy.v1.VmAllocatorApi.DeleteSessionRequest;
import ai.lzy.v1.VmAllocatorApi.FreeRequest;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunning.Operation;
import com.google.protobuf.util.Durations;
import io.fabric8.kubernetes.api.model.PersistentVolume;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeSpec;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static ai.lzy.allocator.model.Volume.AccessMode.READ_WRITE_ONCE;
import static ai.lzy.allocator.test.Utils.waitOperation;
import static ai.lzy.allocator.volume.KuberVolumeManager.KUBER_GB_NAME;
import static ai.lzy.allocator.volume.KuberVolumeManager.VOLUME_CAPACITY_STORAGE_KEY;
import static ai.lzy.allocator.volume.KuberVolumeManager.YCLOUD_DISK_DRIVER;
import static ai.lzy.test.GrpcUtils.withGrpcContext;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static java.util.Objects.requireNonNull;

public class AllocatorServiceTest extends AllocatorApiTestBase {

    private static final int TIMEOUT_SEC = 300;


    @Before
    public void before() throws IOException {
        super.setUp();
    }

    @After
    public void after() {
        super.tearDown();
    }

    @Override
    protected void updateStartupProperties(Map<String, Object> props) {
        super.updateStartupProperties(props);
        props.put("allocator.allocation-timeout", "3s");
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
        kubernetesServer.clearExpectations();
        kubernetesServer.expect().post().withPath(POD_PATH)
            .andReturn(HttpURLConnection.HTTP_INTERNAL_ERROR, new PodListBuilder().build())
            .once();

        var sessionId = createSession(Durations.fromSeconds(100));

        final Operation operation = withGrpcContext(() ->
            authorizedAllocatorBlockingStub.allocate(
                AllocateRequest.newBuilder()
                    .setSessionId(sessionId)
                    .setPoolLabel("S")
                    .setZone(ZONE)
                    .setClusterType(AllocateRequest.ClusterType.USER)
                    .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                    .build()));
        final VmAllocatorApi.AllocateMetadata allocateMeta =
            operation.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        final String podName = getVmPodName(allocateMeta.getVmId());
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        waitOpError(operation, Status.INTERNAL);
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        assertVmMetrics("S", 0, 0, 0);
    }

    @Test
    public void allocateWorkerTimeout() throws Exception {
        // we don't call AllocatorPrivate::registerVm, so we should cancel allocation by timeout

        var sessionId = createSession(Durations.fromSeconds(100));

        final var future = awaitAllocationRequest();

        final Operation operation = withGrpcContext(() ->
            authorizedAllocatorBlockingStub.allocate(
                AllocateRequest.newBuilder()
                    .setSessionId(sessionId)
                    .setPoolLabel("S")
                    .setZone(ZONE)
                    .setClusterType(AllocateRequest.ClusterType.USER)
                    .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                    .build()));

        final String podName = future.get();
        mockGetPod(podName);

        var vmId = operation.getMetadata().unpack(AllocateMetadata.class).getVmId();

        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        waitOpError(operation, Status.DEADLINE_EXCEEDED);
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        assertVmMetrics("S", 0, 0, 0);

        var nr = allocatorCtx.getBean(TestNodeRemover.class);
        Assert.assertTrue(nr.await(Duration.ofSeconds(5)));
        Assert.assertEquals(1, nr.size());
        Assert.assertTrue(nr.contains(vmId, "node", "node"));
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
                    .setClusterType(AllocateRequest.ClusterType.USER)
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
                        .setSessionId(idGenerator.generate("sid-"))
                        .setPoolLabel("S")
                        .setClusterType(AllocateRequest.ClusterType.USER)
                        .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                        .build()),
                TIMEOUT_SEC
            );
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    private AllocatedVm allocateAndFreeVm(String sessionId, Consumer<AllocatedVm> beforeFree) throws Exception {
        var vm = allocateVm(sessionId, null);
        beforeFree.accept(vm);

        freeVm(vm.vmId());
        return vm;
    }

    @Test
    public void allocateFreeSuccess() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        var vm1 = allocateAndFreeVm(
            sessionId,
            vm -> {
                assertVmMetrics("S", -1, 1, 0);
                mockDeletePod(vm.podName(), kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);
            });

        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        assertVmMetrics("S", -1, 0, 0);
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
                .setClusterType(AllocateRequest.ClusterType.USER)
                .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                .build());
        Assert.assertTrue(allocOp.getDone());

        var vmId = allocOp.getMetadata().unpack(AllocateMetadata.class).getVmId();
        Assert.assertEquals(vm1.vmId(), vmId);

        vmId = allocOp.getResponse().unpack(VmAllocatorApi.AllocateResponse.class).getVmId();
        Assert.assertEquals(vm1.vmId(), vmId);

        assertVmMetrics("S", -1, 1, 0);
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
                            .setClusterType(AllocateRequest.ClusterType.USER)
                            .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                            .build());

                    var vmId = allocOp.getMetadata().unpack(AllocateMetadata.class).getVmId();

                    while (vmId.isEmpty()) {
                        LockSupport.parkNanos(Duration.ofMillis(1).toNanos());
                        allocOp = operationServiceApiBlockingStub.get(
                            LongRunning.GetOperationRequest.newBuilder()
                                .setOperationId(allocOp.getId())
                                .build());
                        vmId = allocOp.getMetadata().unpack(AllocateMetadata.class).getVmId();
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
    public void unexpectedlyFreeWhileAllocate() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        final var future = awaitAllocationRequest();

        var allocOp = withGrpcContext(() -> authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .setClusterType(AllocateRequest.ClusterType.USER)
                .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                .build()));

        var vmId = allocOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();

        final String podName = future.get();
        mockGetPod(podName);

        var deleted = new CountDownLatch(1);
        mockDeletePod(podName, deleted::countDown, HttpURLConnection.HTTP_OK);

        assertVmMetrics("S", 1, 0, 0);

        freeVm(vmId);
        Assert.assertTrue(deleted.await(5, TimeUnit.SECONDS));

        allocOp = operationServiceApiBlockingStub.get(
            LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(allocOp.getId())
                .build());
        Assert.assertTrue(allocOp.getDone());
        Assert.assertTrue(allocOp.hasError());
        Assert.assertEquals(Status.Code.CANCELLED.value(), allocOp.getError().getCode());

        assertVmMetrics("S", 0, 0, 0);
    }

    @Test
    public void eventualFreeAfterFail() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(2);

        var vm1 = allocateAndFreeVm(sessionId, vm ->
            mockDeletePod(vm.podName(), () -> {
                kuberRemoveRequestLatch.countDown();
                mockDeletePod(vm.podName(), kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);
            },
            HttpURLConnection.HTTP_INTERNAL_ERROR /* fail the first remove request */));

        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void allocateFromCache() throws Exception {
        var sessionId = createSession(Durations.fromSeconds(10));

        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);

        var cachedVm = allocateAndFreeVm(sessionId,
            vm -> mockDeletePod(vm.podName(), kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK));

        assertVmMetrics("S", -1, 0, 1);

        var operationSecond = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .setClusterType(AllocateRequest.ClusterType.USER)
                .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                .build());
        var allocateMetadataSecond = operationSecond.getMetadata().unpack(AllocateMetadata.class);
        operationSecond = waitOpSuccess(operationSecond);

        final var allocateResponseSecond = operationSecond.getResponse().unpack(AllocateResponse.class);
        Assert.assertEquals(cachedVm.vmId(), allocateResponseSecond.getVmId());

        Assert.assertEquals(cachedVm.vmId(), allocateMetadataSecond.getVmId());

        assertVmMetrics("S", -1, 1, 0);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(
            FreeRequest.newBuilder().setVmId(allocateMetadataSecond.getVmId()).build());

        Assert.assertEquals(cachedVm.vmId(), allocateMetadataSecond.getVmId());
        assertVmMetrics("S", -1, 0, 1);

        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));

        assertVmMetrics("S", 0, 0, 0);
    }

    @Test
    public void allocateConcurrentFromCache() throws Exception {
        var sessionId = createSession(Durations.fromSeconds(5000));

        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);

        var cachedVm = allocateAndFreeVm(sessionId,
            vm -> mockDeletePod(vm.podName(), kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK));

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
                            .setClusterType(AllocateRequest.ClusterType.USER)
                            .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                            .build());

                    var allocResp = allocOp.getMetadata().unpack(AllocateMetadata.class);
                    allocatedVmIds[index] = allocResp.getVmId();
                } catch (Throwable e) {
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

        var n = Arrays.stream(allocatedVmIds).filter(cachedVm.vmId()::equals).count();
        Assert.assertEquals(1, n);
    }

    @Test
    public void deleteSessionParallelToAllocation() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        var deleteLatch = new CountDownLatch(1);
        var deletedLatch = new CountDownLatch(1);

        var deleteSessionThread = new Thread(() -> {
            try {
                deleteLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            var op = authorizedAllocatorBlockingStub.deleteSession(
                DeleteSessionRequest.newBuilder()
                    .setSessionId(sessionId)
                    .build());
            waitOpSuccess(op);
            deletedLatch.countDown();
        }, "delete-session");
        deleteSessionThread.start();

        InjectedFailures.FAIL_ALLOCATE_VMS.get(10).set(() -> {
            deleteLatch.countDown();
            try {
                deletedLatch.await();
                System.out.println("--> continue allocation");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        try {
            authorizedAllocatorBlockingStub.allocate(
                AllocateRequest.newBuilder()
                    .setSessionId(sessionId)
                    .setPoolLabel("S")
                    .setZone(ZONE)
                    .setClusterType(AllocateRequest.ClusterType.USER)
                    .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.Code.INTERNAL, e.getStatus().getCode());
            Assert.assertEquals("Session %s not found".formatted(sessionId), e.getStatus().getDescription());
        }

        deleteSessionThread.join();
    }

    @Test
    public void deleteSessionWithActiveVmsAfterRegister() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        var future = awaitAllocationRequest();

        var allocate = withGrpcContext(() ->
            authorizedAllocatorBlockingStub.allocate(
                AllocateRequest.newBuilder()
                    .setSessionId(sessionId)
                    .setPoolLabel("S")
                    .setZone(ZONE)
                    .setClusterType(AllocateRequest.ClusterType.USER)
                    .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                    .build()));
        var allocateMetadata = allocate.getMetadata().unpack(AllocateMetadata.class);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        String clusterId = withGrpcContext(() ->
            requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId());
        registerVm(allocateMetadata.getVmId(), clusterId);

        deleteSession(sessionId, true);

        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
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
                .setClusterType(AllocateRequest.ClusterType.USER)
                .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                .build());
        var allocateMetadata = allocate.getMetadata().unpack(AllocateMetadata.class);

        var vm = vmDao.get(allocateMetadata.getVmId(), null);
        Assert.assertNotNull(vm);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        var op = authorizedAllocatorBlockingStub.deleteSession(
            DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId)
                .build());
        Assert.assertFalse(op.getDone());

        try {
            //noinspection ResultOfMethodCallIgnored
            privateAllocatorBlockingStub
                .withInterceptors(OttHelper.createOttClientInterceptor(vm.vmId(), vm.allocateState().vmOtt()))
                .register(
                    VmAllocatorPrivateApi.RegisterRequest.newBuilder()
                        .setVmId(allocateMetadata.getVmId())
                        .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            switch (e.getStatus().getCode()) {
                case CANCELLED ->
                    Assert.assertEquals(e.getStatus().toString(),
                        "Op %s already done".formatted(allocate.getId()),
                        e.getStatus().getDescription());
                case NOT_FOUND -> { }
                case FAILED_PRECONDITION ->
                    Assert.assertEquals(e.getStatus().toString(),
                        "Unexpected VM status DELETING",
                        e.getStatus().getDescription());
                default -> Assert.fail("Unexpected status: " + e.getStatus());
            }
        }

        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void freeNonexistentVm() {
        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.free(
                FreeRequest.newBuilder().setVmId(idGenerator.generate("vm-fake-")).build());
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
            vm -> mockDeletePod(vm.podName(), kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK));

        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(vm1.vmId()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != Status.NOT_FOUND.getCode() &&
                e.getStatus().getCode() != Status.FAILED_PRECONDITION.getCode())
            {
                Assert.fail(e.getStatus().toString());
            }
        }
        Assert.assertTrue(kuberRemoveRequestLatch.await(TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void repeatedWorkerRegister() throws Exception {
        var sessionId = createSession(Durations.ZERO);

        var future = awaitAllocationRequest();

        var allocate = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .setClusterType(AllocateRequest.ClusterType.USER)
                .addWorkload(AllocateRequest.Workload.getDefaultInstance())
                .build());
        var allocateMetadata = allocate.getMetadata().unpack(AllocateMetadata.class);

        var vm = vmDao.get(allocateMetadata.getVmId(), null);
        Assert.assertNotNull(vm);

        final String podName = future.get();
        mockGetPod(podName);
        final CountDownLatch kuberRemoveRequestLatch = new CountDownLatch(1);
        mockDeletePod(podName, kuberRemoveRequestLatch::countDown, HttpURLConnection.HTTP_OK);

        String clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        registerVm(allocateMetadata.getVmId(), clusterId);

        try {
            //noinspection ResultOfMethodCallIgnored
            privateAllocatorBlockingStub
                .withInterceptors(OttHelper.createOttClientInterceptor(vm.vmId(), vm.allocateState().vmOtt()))
                .register(
                    VmAllocatorPrivateApi.RegisterRequest.newBuilder()
                        .setVmId(allocateMetadata.getVmId())
                        .build());
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
                .setClusterType(AllocateRequest.ClusterType.USER)
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
        var allocateMetadata = allocationStarted.getMetadata().unpack(AllocateMetadata.class);

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
                    .setClusterType(AllocateRequest.ClusterType.USER)
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

    @Test
    public void mountHandleWontWorkIfMountsAreDisabled() {
        var exception = Assert.assertThrows(StatusRuntimeException.class, () -> {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.mount(VmAllocatorApi.MountRequest.getDefaultInstance());
        });
        Assert.assertEquals(Status.UNIMPLEMENTED.getCode(), exception.getStatus().getCode());
    }

    @Test
    public void unmountHandleWontWorkIfMountsAreDisabled() {
        var exception = Assert.assertThrows(StatusRuntimeException.class, () -> {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.unmount(VmAllocatorApi.UnmountRequest.getDefaultInstance());
        });
        Assert.assertEquals(Status.UNIMPLEMENTED.getCode(), exception.getStatus().getCode());
    }
}
