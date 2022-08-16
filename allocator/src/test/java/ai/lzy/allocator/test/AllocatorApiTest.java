package ai.lzy.allocator.test;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi.CachePolicy;
import ai.lzy.v1.VmAllocatorApi.CreateSessionRequest;
import ai.lzy.v1.VmAllocatorApi.CreateSessionResponse;
import ai.lzy.v1.VmAllocatorApi.DeleteSessionRequest;
import ai.lzy.v1.VmAllocatorApi.DeleteSessionResponse;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Duration;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import java.io.IOException;
import java.util.UUID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AllocatorApiTest {

    private ApplicationContext allocatorCtx;
    private AllocatorGrpc.AllocatorBlockingStub allocatorBlockingStub;
    private AllocatorMain allocatorApp;

    @Before
    public void before() throws IOException {
        allocatorCtx = ApplicationContext.run();
        allocatorApp = allocatorCtx.getBean(AllocatorMain.class);
        allocatorApp.start();

        final var config = allocatorCtx.getBean(ServiceConfig.class);
        //noinspection UnstableApiUsage
        final var channel = ChannelBuilder
            .forAddress(HostAndPort.fromString(config.address()))
            .usePlaintext()
            .build();
        allocatorBlockingStub = AllocatorGrpc.newBlockingStub(channel);
    }

    @After
    public void after() {
        allocatorApp.stop();
        try {
            allocatorApp.awaitTermination();
        } catch (InterruptedException ignored) {
            // ignored
        }
        allocatorCtx.stop();
    }

    @Test
    public void testCreateAndDeleteSession() {
        final CreateSessionResponse createSessionResponse = allocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(100).build()).build())
                .build());
        final DeleteSessionResponse deleteSessionResponse = allocatorBlockingStub.deleteSession(
            DeleteSessionRequest.newBuilder().setSessionId(createSessionResponse.getSessionId()).build());

        Assert.assertNotNull(createSessionResponse.getSessionId());
        Assert.assertNotNull(deleteSessionResponse);
    }

    @Test
    public void createSessionNoOwner() {
        try {
            //noinspection ResultOfMethodCallIgnored
            allocatorBlockingStub.createSession(CreateSessionRequest.newBuilder().setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(100).build()).build())
                .build());
            Assert.fail();
        } catch (
            StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void createSessionNoCachePolicy() {
        try {
            //noinspection ResultOfMethodCallIgnored
            allocatorBlockingStub.createSession(
                CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).build());
            Assert.fail();
        } catch (
            StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void createSessionNoCachePolicyDuration() {
        try {
            //noinspection ResultOfMethodCallIgnored
            allocatorBlockingStub.createSession(
                CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().build()).build());
            Assert.fail();
        } catch (
            StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }
}
