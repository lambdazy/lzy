package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.AllocatorMetrics;
import ai.lzy.allocator.gc.GarbageCollector;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.util.Durations;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.grpc.Status;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static java.util.Objects.requireNonNull;

public class AllocatorServiceMetricsTest extends AllocatorApiTestBase {

    private AllocatorMetrics metrics;
    private GarbageCollector gc;
    private String sessionId;

    @Before
    public void before() throws IOException {
        super.setUp();

        metrics = allocatorCtx.getBean(AllocatorMetrics.class);
        gc = allocatorCtx.getBean(GarbageCollector.class);

        sessionId = createSession(Durations.fromSeconds(0));
        Assert.assertEquals(1, (int) metrics.activeSessions.get());
    }

    @After
    public void after() {
        super.tearDown();
    }

    @Override
    protected void updateStartupProperties(Map<String, Object> props) {
        props.put("allocator.allocation-timeout", "10m");
        props.put("allocator.gc.initial-delay", "100m");
        props.put("allocator.gc.cleanup-period", "100m");
        props.put("allocator.gc.lease-duration", "1000m");
        props.put("allocator.gc.graceful-shutdown-duration", "0s");
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
    @Ignore
    public void metrics2() throws Exception {
        var vmId = allocateVm();

        authorizedAllocatorBlockingStub.deleteSession(
            VmAllocatorApi.DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId)
                .build());
        Assert.assertEquals(0, (int) metrics.activeSessions.get());

        gc.forceRun();

        Assert.assertEquals(0, (int) metrics.runningAllocations.labels("S").get());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(0, (int) metrics.cachedVms.labels("S").get());
    }

    // createSession()
    //   do allocate/free/deleteSession() VM and observe runningAllocations/runningVms/cachedVms metrics
    @Test
    @Ignore
    public void metrics3() throws Exception {
        var vmId = allocateVm();

        authorizedAllocatorBlockingStub.free(
            VmAllocatorApi.FreeRequest.newBuilder()
                .setVmId(vmId)
                .build());

        authorizedAllocatorBlockingStub.deleteSession(
            VmAllocatorApi.DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId)
                .build());
        Assert.assertEquals(0, (int) metrics.activeSessions.get());

        gc.forceRun();

        Assert.assertEquals(0, (int) metrics.runningAllocations.labels("S").get());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(0, (int) metrics.cachedVms.labels("S").get());
    }

    // createSession()
    //  do allocateVm(), it fails
    @Test
    public void metrics4() {
        var latch = new CountDownLatch(1);

        kubernetesServer.expect().post()
            .withPath(POD_PATH)
            .andReply(HttpURLConnection.HTTP_INTERNAL_ERROR, req -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return new PodListBuilder().build();
            })
            .once();

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
        var awaitAllocFuture = awaitAllocationRequest(latch);

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

        var podName = awaitAllocFuture.get();
        mockGetPod(podName);

        var clusterId = requireNonNull(clusterRegistry.findCluster("S", "test-zone", CLUSTER_TYPE)).clusterId();
        registerVm(vmId, clusterId);

        allocOp = waitOpSuccess(allocOp);
        Assert.assertEquals(vmId, allocOp.getResponse().unpack(VmAllocatorApi.AllocateResponse.class).getVmId());

        Assert.assertEquals(0, (int) metrics.runningAllocations.labels("S").get());
        Assert.assertEquals(1, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(0, (int) metrics.cachedVms.labels("S").get());

        mockDeletePod(podName, () -> {}, HttpURLConnection.HTTP_OK);

        return vmId;
    }
}
