package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.gc.GarbageCollector;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.allocator.services.AllocatorService;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.Durations;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.grpc.Status;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

public class CancelAllocationTest extends AllocatorApiTestBase {

    private static final String ZONE = "test-zone";

    @Before
    public void before() throws IOException {
        super.setUp();
        InjectedFailures.reset();
    }

    @After
    public void after() {
        super.tearDown();
        InjectedFailures.reset();
    }

    @Override
    protected void updateStartupProperties(Map<String, Object> props) {
        props.put("allocator.allocation-timeout", "10m");
        props.put("allocator.gc.initial-delay", "100m");
        props.put("allocator.gc.cleanup-period", "100m");
        props.put("allocator.gc.lease-duration", "1000m");
        props.put("allocator.gc.graceful-shutdown-duration", "0s");
    }

    private record AllocVm(
        String vmId,
        String allocOpId
    ) {}

    private void assertVmCleaned(AllocVm x) throws Exception {
        var vmDao = allocatorCtx.getBean(VmDao.class);
        var vm = vmDao.get(x.vmId, null);
        Assert.assertNull(vm);

        var operationsDao = allocatorCtx.getBean(OperationDao.class);
        var allocOp = operationsDao.get(x.allocOpId, null);
        Assert.assertNull(allocOp);

        var vmIamSubject = allocatorCtx.getBean(SubjectServiceGrpcClient.class)
            .findSubject(AuthProvider.INTERNAL, x.vmId, SubjectType.VM);
        Assert.assertNull(vmIamSubject);
    }

    @Test
    public void cancelAllocate0() throws Exception {
        var latch = new CountDownLatch(1);

        InjectedFailures.FAIL_ALLOCATE_VMS.get(0).set(waitOn(latch));

        var vm = allocateImpl((__, op) -> {
            var resp = operationServiceApiBlockingStub.cancel(
                LongRunning.CancelOperationRequest.newBuilder()
                    .setOperationId(op.getId())
                    .build());
            System.err.println("--> cancel: " + TextFormat.printer().shortDebugString(resp.getError()));
            latch.countDown();
            return false;
        });

        allocatorCtx.getBean(GarbageCollector.class).forceRun();
        assertVmCleaned(vm);
    }

    @Test
    public void cancelAllocate0WithRestart() throws Exception {
        var latch = new CountDownLatch(1);

        InjectedFailures.FAIL_ALLOCATE_VMS.get(0).set(waitOn(latch));

        var vm = allocateImpl((__, op) -> {
            var resp = operationServiceApiBlockingStub.cancel(
                LongRunning.CancelOperationRequest.newBuilder()
                    .setOperationId(op.getId())
                    .build());
            System.err.println("--> cancel: " + TextFormat.printer().shortDebugString(resp.getError()));
            latch.countDown();
            return true;
        });

        allocatorCtx.getBean(GarbageCollector.class).forceRun();
        assertVmCleaned(vm);
    }

    @Test
    public void cancelAllocate2() throws Exception {
        var latch = new CountDownLatch(1);

        InjectedFailures.FAIL_ALLOCATE_VMS.get(2).set(waitOn(latch));

        var vm = allocateImpl((__, op) -> {
            var resp = operationServiceApiBlockingStub.cancel(
                LongRunning.CancelOperationRequest.newBuilder()
                    .setOperationId(op.getId())
                    .build());
            System.err.println("--> cancel: " + TextFormat.printer().shortDebugString(resp.getError()));
            latch.countDown();
            return false;
        });

        allocatorCtx.getBean(GarbageCollector.class).forceRun();
        assertVmCleaned(vm);
    }

    @Test
    public void cancelAllocate2WithRestart() throws Exception {
        var latch = new CountDownLatch(1);

        InjectedFailures.FAIL_ALLOCATE_VMS.get(2).set(waitOnAndTerminate(latch, "term"));

        var vm = allocateImpl((__, op) -> {
            var resp = operationServiceApiBlockingStub.cancel(
                LongRunning.CancelOperationRequest.newBuilder()
                    .setOperationId(op.getId())
                    .build());
            System.err.println("--> cancel: " + TextFormat.printer().shortDebugString(resp.getError()));
            latch.countDown();
            return true;
        });

        allocatorCtx.getBean(GarbageCollector.class).forceRun();
        assertVmCleaned(vm);
    }

    private AllocVm allocateImpl(BiFunction<String, LongRunning.Operation, Boolean> action) throws Exception {
        final var createdPod = new CompletableFuture<String>();

        kubernetesServer.expect().post()
            .withPath(POD_PATH)
            .andReply(HttpURLConnection.HTTP_CREATED, (req) -> {
                final var pod = Serialization.unmarshal(
                    new ByteArrayInputStream(req.getBody().readByteArray()), Pod.class, Map.of());
                createdPod.complete(pod.getMetadata().getName());
                return pod;
            })
            .once();

        var sessionId = createSession(Durations.ZERO);

        var allocOp = authorizedAllocatorBlockingStub.allocate(
            VmAllocatorApi.AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .setClusterType(VmAllocatorApi.AllocateRequest.ClusterType.USER)
                .addWorkload(
                    VmAllocatorApi.AllocateRequest.Workload.newBuilder()
                        .setName("wl")
                        .putEnv("k", "v")
                        .build())
                .build());
        Assert.assertFalse(allocOp.getDone());
        var vmId = allocOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();

        if (action.apply(vmId, allocOp)) {
            allocatorCtx.getBean(AllocatorService.class).testRestart(false);
        }

        allocOp = operationServiceApiBlockingStub.get(
            LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(allocOp.getId())
                .build());

        if (allocOp.getDone()) {
            Assert.assertTrue(allocOp.toString(), allocOp.hasError());
            Assert.assertEquals(Status.Code.CANCELLED.value(), allocOp.getError().getCode());
            Assert.assertFalse(createdPod.isDone());
            return new AllocVm(vmId, allocOp.getId());
        }

        final String podName = createdPod.get();
        mockGetPod(podName);

        var clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        registerVm(vmId, clusterId);

        allocOp = waitOpSuccess(allocOp);
        Assert.assertEquals(vmId, allocOp.getResponse().unpack(VmAllocatorApi.AllocateResponse.class).getVmId());

        return new AllocVm(vmId, allocOp.getId());
    }

    private static Runnable waitOn(CountDownLatch latch) {
        return () -> {
            System.err.println("--> wait on " + latch);
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.err.println("--> wait on " + latch + " completed");
        };
    }

    private static Runnable waitOnAndTerminate(CountDownLatch latch, String message) {
        return () -> {
            System.err.println("--> wait on " + latch);
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.err.println("--> wait on " + latch + " completed");
            throw new InjectedFailures.TerminateException(message);
        };
    }
}
