package ai.lzy.allocator.test;

import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.allocator.services.AllocatorService;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.util.Durations;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.fabric8.mockwebserver.utils.ResponseProvider;
import okhttp3.Headers;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.lzy.allocator.test.Utils.waitOperation;
import static java.util.Objects.requireNonNull;

public class RestartAllocatorTest extends AllocatorApiTestBase {

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
        props.put("allocator.gc.cleanup-period", "100m");
        props.put("allocator.gc.lease-duration", "1000m");
    }

    @Test
    public void allocateVmFail1() throws Exception {
        InjectedFailures.FAIL_ALLOCATE_VMS.get(1).set(() -> new InjectedFailures.TerminateException("term 1"));
        allocateVmFailImpl();
    }

    @Test
    public void allocateVmFail2() throws Exception {
        InjectedFailures.FAIL_ALLOCATE_VMS.get(2).set(() -> new InjectedFailures.TerminateException("term 2"));
        allocateVmFailImpl();
    }

    @Test
    public void allocateVmFail3() throws Exception {
        InjectedFailures.FAIL_ALLOCATE_VMS.get(3).set(() -> new InjectedFailures.TerminateException("term 3"));
        allocateVmFailImpl();
    }

    @Test
    @Ignore("requires v6 tunnel proxy to be set")
    public void allocateVmFail4() throws Exception {
        InjectedFailures.FAIL_ALLOCATE_VMS.get(4).set(() -> new InjectedFailures.TerminateException("term 4"));
        allocateVmFailImpl();
    }

    @Test
    public void allocateVmFail5() throws Exception {
        InjectedFailures.FAIL_ALLOCATE_VMS.get(5).set(() -> new InjectedFailures.TerminateException("term 5"));
        allocateVmFailImpl();
    }

    @Test
    public void allocateVmFail6() throws Exception {
        InjectedFailures.FAIL_ALLOCATE_VMS.get(6).set(() -> new InjectedFailures.TerminateException("term 6"));
        allocateVmFailImpl();
    }

    @Test
    @Ignore("requires v6 tunnel proxy to be set")
    public void allocateVmFail7() throws Exception {
        InjectedFailures.FAIL_ALLOCATE_VMS.get(7).set(() -> new InjectedFailures.TerminateException("term 7"));
        allocateVmFailImpl();
    }

    @Test
    @Ignore("double call to k8s")
    public void allocateVmFail8() throws Exception {
        InjectedFailures.FAIL_ALLOCATE_VMS.get(8).set(() -> new InjectedFailures.TerminateException("term 8"));
        allocateVmFailImpl();
    }

    private void allocateVmFailImpl() throws Exception {
        final var failKuberAlloc = new AtomicBoolean(true);
        final var createdPod = new CompletableFuture<String>();

        kubernetesServer.expect()
            .post().withPath(POD_PATH).andReply(new ResponseProvider<>() {
                private Headers headers = new Headers.Builder().build();

                @Override
                public Object getBody(RecordedRequest request) {
                    Assert.assertFalse(failKuberAlloc.get());
                    final var pod = Serialization.unmarshal(
                        new ByteArrayInputStream(request.getBody().readByteArray()), Pod.class, Map.of());
                    createdPod.complete(pod.getMetadata().getName());
                    return pod;
                }

                @Override
                public int getStatusCode(RecordedRequest request) {
                    Assert.assertFalse(failKuberAlloc.get());
                    return HttpURLConnection.HTTP_CREATED;
                }

                @Override
                public Headers getHeaders() {
                    return headers;
                }

                @Override
                public void setHeaders(Headers headers) {
                    this.headers = headers;
                }
            })
            .once();

        var sessionId = createSession(Durations.ZERO);

        failKuberAlloc.set(true);

        var allocOp = authorizedAllocatorBlockingStub.allocate(
            VmAllocatorApi.AllocateRequest.newBuilder()
                .setSessionId(sessionId)
                .setPoolLabel("S")
                .setZone(ZONE)
                .addWorkload(
                    VmAllocatorApi.AllocateRequest.Workload.newBuilder()
                        .setName("wl")
                        .putEnv("k", "v")
                        .build())
                .build());
        Assert.assertFalse(allocOp.getDone());
        var vmId = allocOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();

        allocOp = waitOperation(operationServiceApiBlockingStub, allocOp, 3);
        Assert.assertFalse(allocOp.getDone());
        Assert.assertFalse(createdPod.isDone());

        // restart AllocatorService

        failKuberAlloc.set(false);

        allocatorCtx.getBean(AllocatorService.class).testRestart(false);

        final String podName = createdPod.get();
        mockGetPod(podName);

        var clusterId = requireNonNull(clusterRegistry.findCluster("S", ZONE, CLUSTER_TYPE)).clusterId();
        registerVm(vmId, clusterId);

        allocOp = waitOpSuccess(allocOp);
        Assert.assertEquals(vmId, allocOp.getResponse().unpack(VmAllocatorApi.AllocateResponse.class).getVmId());
    }
}
