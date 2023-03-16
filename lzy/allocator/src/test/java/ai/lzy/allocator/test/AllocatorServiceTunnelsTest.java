package ai.lzy.allocator.test;

import ai.lzy.test.GrpcUtils;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.AllocateMetadata;
import ai.lzy.v1.VmAllocatorApi.AllocateRequest;
import ai.lzy.v1.VmAllocatorApi.FreeRequest;
import ai.lzy.v1.longrunning.LongRunning.Operation;
import com.google.protobuf.util.Durations;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class AllocatorServiceTunnelsTest extends AllocatorApiTestBase {

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
        props.put("allocator.allocation-timeout", "30s");
        props.put("allocator.tunnel.agent-port", GrpcUtils.rollPort());
    }

    @Test
    public void proxyV6AddressIsValidated() {
        var sessionId = createSession(Durations.ZERO);
        var exception = Assert.assertThrows(StatusRuntimeException.class, () -> {
            var ignored = authorizedAllocatorBlockingStub.allocate(
                AllocateRequest.newBuilder()
                    .setSessionId(sessionId)
                    .setPoolLabel("S")
                    .setZone(ZONE)
                    .setClusterType(AllocateRequest.ClusterType.USER)
                    .addWorkload(AllocateRequest.Workload.newBuilder()
                        .setName("workload")
                        .build())
                    .setTunnelSettings(VmAllocatorApi.TunnelSettings.newBuilder()
                        .setProxyV6Address("1.1.1.1")
                        .build())
                    .build());
        });
        Assert.assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
        Assert.assertEquals("Tunnel settings errors: 'Address 1.1.1.1 isn't v6!'",
            exception.getStatus().getDescription());
    }

    @Test
    public void allocatorShouldAllocateTunnelWithTunnelSettings() throws Exception {
        var sessionId = createSession(Durations.ZERO);
        allocateWithTunnel(sessionId);
    }

    @Test
    public void allocatorShouldDestroyTunnelOnFree() throws Exception {
        var sessionId = createSession(Durations.ZERO);
        var vmId = allocateWithTunnel(sessionId);

        var countDownLatch = new CountDownLatch(1);

        mockGetPodByName(getTunnelPodName(vmId));
        mockDeletePodByName(getTunnelPodName(vmId), () -> {}, HttpURLConnection.HTTP_OK);
        mockGetPod(getVmPodName(vmId));
        mockDeletePod(getVmPodName(vmId), countDownLatch::countDown, HttpURLConnection.HTTP_OK);
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder()
            .setVmId(vmId)
            .build());
        Assert.assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        Mockito.verify(mockTunnelManager, Mockito.times(1)).destroyTunnel();
    }

    private String allocateWithTunnel(String sessionId) throws Exception {
        Future<String> tunnelPodFuture = awaitAllocationRequest();
        Future<String> vmPodFuture = awaitAllocationRequest();
        Operation operation = authorizedAllocatorBlockingStub.allocate(
            AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .setClusterType(AllocateRequest.ClusterType.USER)
                .addWorkload(AllocateRequest.Workload.newBuilder()
                    .setName("workload")
                    .build())
                .setTunnelSettings(VmAllocatorApi.TunnelSettings.newBuilder()
                    .setProxyV6Address("fe80::")
                    .setTunnelIndex(42)
                    .build())
                .build());
        var allocateMetadata = operation.getMetadata().unpack(AllocateMetadata.class);
        String clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        vmPodFuture.get();
        mockGetPod(getVmPodName(allocateMetadata.getVmId()));
        registerVm(allocateMetadata.getVmId(), clusterId);

        waitOpSuccess(operation);

        Assert.assertEquals(getTunnelPodName(allocateMetadata.getVmId()), tunnelPodFuture.get());
        return allocateMetadata.getVmId();
    }
}
