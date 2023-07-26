package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.RestoreOperations;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.test.TimeUtils;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.Durations;
import io.grpc.Status;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static ai.lzy.test.GrpcUtils.withGrpcContext;
import static java.util.Objects.requireNonNull;

public class CancelAllocationTest extends AllocatorApiTestBase {
    private static final String ZONE = "test-zone";

    @Before
    public void before() throws IOException {
        InjectedFailures.reset();
    }

    @After
    public void after() {
        InjectedFailures.reset();
    }

    private record AllocVm(String vmId, String allocOpId) {}

    private void assertVmCleaned(AllocVm x) throws Exception {
        var vmDao = allocatorContext.getBean(VmDao.class);
        var vm = vmDao.get(x.vmId, null);
        Assert.assertNull(vm);

        Assert.assertTrue(vmDao.hasDeadVm(x.vmId));

        var operationsDao = allocatorContext.getBean(OperationDao.class);
        var allocOp = operationsDao.get(x.allocOpId, null);
        Assert.assertEquals(Status.CANCELLED.getCode(),
            Status.fromCodeValue(allocOp.error().getCode().value()).getCode());
    }

    @Test
    public void cancelAllocate0() throws Exception {
        var latch = new CountDownLatch(1);

        InjectedFailures.FAIL_ALLOCATE_VMS.get(0).set(waitOn(latch));

        final var vm = allocateImpl((__, op) -> {
            var resp = operationServiceApiBlockingStub.cancel(
                LongRunning.CancelOperationRequest.newBuilder()
                    .setOperationId(op.getId())
                    .build());
            System.err.println("--> cancel: " + TextFormat.printer().shortDebugString(resp.getError()));
            latch.countDown();
            return false;
        });

        waitVm(vm);
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

        waitVm(vm);
        assertVmCleaned(vm);
    }

    private AllocVm allocateImpl(BiFunction<String, LongRunning.Operation, Boolean> action) throws Exception {
        final var createdPod = mockCreatePod();

        var sessionId = createSession(Durations.ZERO);

        var allocOp = withGrpcContext(() -> authorizedAllocatorBlockingStub.allocate(
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
                .build()));
        Assert.assertFalse(allocOp.getDone());
        var vmId = allocOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();

        if (action.apply(vmId, allocOp)) {
            System.err.println("--> do restart ...");
            operationsExecutor.dropAll();
            allocatorContext.getBean(RestoreOperations.class); //calls restore after construction
            System.err.println("--> restart done");
        }

        var opId = allocOp.getId();
        allocOp = withGrpcContext(() -> operationServiceApiBlockingStub.get(
            LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(opId)
                .build()));

        if (allocOp.getDone()) {
            Assert.assertTrue(allocOp.toString(), allocOp.hasError());
            Assert.assertEquals(Status.Code.CANCELLED.value(), allocOp.getError().getCode());
            Assert.assertFalse(createdPod.isDone());
            return new AllocVm(vmId, allocOp.getId());
        }

        Assert.fail();

        final String podName = getName(createdPod.get());
        mockGetPodByName(podName);

        var clusterId = requireNonNull(withGrpcContext(() ->
            clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId());
        registerVm(vmId, clusterId);

        allocOp = waitOpSuccess(allocOp);
        Assert.assertEquals(vmId, allocOp.getResponse().unpack(VmAllocatorApi.AllocateResponse.class).getVmId());

        return new AllocVm(vmId, allocOp.getId());
    }

    private void waitVm(AllocVm vm) {
        var done = TimeUtils.waitFlagUp(() -> {
            var vmDao = allocatorContext.getBean(VmDao.class);
            try {
                return vmDao.get(vm.vmId, null) == null;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, 5, TimeUnit.SECONDS);
        Assert.assertTrue(done);
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
