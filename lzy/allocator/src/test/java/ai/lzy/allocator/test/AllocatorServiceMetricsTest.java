package ai.lzy.allocator.test;

import ai.lzy.test.TimeUtils;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.util.Durations;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.grpc.Status;
import okhttp3.mockwebserver.MockResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static ai.lzy.allocator.test.http.RequestMatchers.exactPath;
import static java.util.Objects.requireNonNull;

public class AllocatorServiceMetricsTest extends AllocatorApiTestBase {
    private String sessionId;

    @Before
    public void before() {
        sessionId = createSession(Durations.fromMillis(1));
        //Assert.assertEquals(1, (int) metrics.activeSessions.get());
    }

    @Override
    protected Map<String, Object> allocatorConfigOverrides() {
        return Map.of(
            "allocator.allocation-timeout", "10m",
            "allocator.gc.initial-delay", "100m",
            "allocator.gc.cleanup-period", "100m",
            "allocator.gc.lease-duration", "1000m",
            "allocator.gc.graceful-shutdown-duration", "0s"
        );
    }

    // createSession()
    //   do allocate/free/delete VM and observe runningAllocations/runningVms/cachedVms metrics
    // deleteSession()
    @Test
    public void metrics1() throws Exception {
        var vmId = allocateVm();

        authorizedAllocatorBlockingStub.free(
            VmAllocatorApi.FreeRequest.newBuilder()
                .setVmId(vmId)
                .build());

        Assert.assertEquals(0, (int) metrics.runningAllocations.labels("S").get());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(1, (int) metrics.cachedVms.labels("S").get());

        gc.forceRun();

        Assert.assertEquals(0, (int) metrics.runningAllocations.labels("S").get());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(0, (int) metrics.cachedVms.labels("S").get());

        authorizedAllocatorBlockingStub.deleteSession(
            VmAllocatorApi.DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId)
                .build());
        Assert.assertEquals(0, (int) metrics.activeSessions.get());
    }

    // createSession()
    //   do allocate/deleteSession() VM and observe runningAllocations/runningVms/cachedVms metrics
    @Test
    public void metrics2() throws Exception {
        var vmId = allocateVm();

        var op = authorizedAllocatorBlockingStub.deleteSession(
            VmAllocatorApi.DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId)
                .build());
        waitOpSuccess(op);
        Assert.assertEquals(0, (int) metrics.activeSessions.get());

        TimeUtils.waitFlagUp(() -> operationsExecutor.operationsCount() == 0, 5, TimeUnit.SECONDS);

        Assert.assertEquals(0, (int) metrics.runningAllocations.labels("S").get());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(0, (int) metrics.cachedVms.labels("S").get());
    }

    // createSession()
    //   do allocate/free/deleteSession() VM and observe runningAllocations/runningVms/cachedVms metrics
    @Test
    public void metrics3() throws Exception {
        var vmId = allocateVm();

        authorizedAllocatorBlockingStub.free(
            VmAllocatorApi.FreeRequest.newBuilder()
                .setVmId(vmId)
                .build());

        var op = authorizedAllocatorBlockingStub.deleteSession(
            VmAllocatorApi.DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId)
                .build());
        waitOpSuccess(op);

        Assert.assertEquals(0, (int) metrics.activeSessions.get());

        TimeUtils.waitFlagUp(() -> operationsExecutor.operationsCount() == 0, 5, TimeUnit.SECONDS);

        Assert.assertEquals(0, (int) metrics.runningAllocations.labels("S").get());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(0, (int) metrics.cachedVms.labels("S").get());
    }

    // createSession()
    //  do allocateVm(), it fails
    @Test
    public void metrics4() {
        var latch = new CountDownLatch(1);

        mockRequestDispatcher.addHandlerOneTime(exactPath(POD_PATH), req -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return new MockResponse().setBody(toJson(new PodListBuilder().build()))
                .setResponseCode(HttpURLConnection.HTTP_INTERNAL_ERROR);
        });

        var allocOp = authorizedAllocatorBlockingStub.allocate(
            VmAllocatorApi.AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone("test-zone")
                .setClusterType(VmAllocatorApi.AllocateRequest.ClusterType.USER)
                .addWorkload(VmAllocatorApi.AllocateRequest.Workload.getDefaultInstance())
                .build());
        Assert.assertFalse(allocOp.getDone());

        Assert.assertEquals(1, (int) metrics.runningAllocations.labels("S").get());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(0, (int) metrics.cachedVms.labels("S").get());

        latch.countDown();
        waitOpError(allocOp, Status.INTERNAL);

        Assert.assertEquals(0, (int) metrics.runningAllocations.labels("S").get());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(0, (int) metrics.cachedVms.labels("S").get());
    }

    private String allocateVm() throws Exception {
        var latch = new CountDownLatch(1);
        var awaitAllocFuture = mockCreatePod(podName -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        var allocOp = authorizedAllocatorBlockingStub.allocate(
            VmAllocatorApi.AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone("test-zone")
                .setClusterType(VmAllocatorApi.AllocateRequest.ClusterType.USER)
                .addWorkload(VmAllocatorApi.AllocateRequest.Workload.getDefaultInstance())
                .build());
        Assert.assertFalse(allocOp.getDone());
        var vmId = allocOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();

        Assert.assertEquals(1, (int) metrics.runningAllocations.labels("S").get());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(0, (int) metrics.cachedVms.labels("S").get());

        latch.countDown();

        var podName = getName(awaitAllocFuture.get());
        mockGetPodByName(podName);

        var clusterId = requireNonNull(clusterRegistry.findCluster("S", "test-zone", CLUSTER_TYPE)).clusterId();
        registerVm(vmId, clusterId);

        allocOp = waitOpSuccess(allocOp);
        Assert.assertEquals(vmId, allocOp.getResponse().unpack(VmAllocatorApi.AllocateResponse.class).getVmId());

        // we need some time to complete running action
        TimeUtils.waitFlagUp(() -> metrics.runningAllocations.labels("S").get() == 0, 3, TimeUnit.SECONDS);

        Assert.assertEquals(0, (int) metrics.runningAllocations.labels("S").get());
        Assert.assertEquals(1, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(0, (int) metrics.cachedVms.labels("S").get());

        mockDeletePodByName(podName, () -> {}, HttpURLConnection.HTTP_OK);

        return vmId;
    }
}
