package ai.lzy.allocator.test;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactory;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.test.TimeUtils;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.*;
import ai.lzy.v1.OperationService.GetOperationRequest;
import ai.lzy.v1.OperationService.Operation;
import ai.lzy.v1.VmAllocatorApi.*;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class AllocatorApiTest extends BaseTestWithIam {

    private static final int TIMEOUT_SEC = 300;

    private ApplicationContext allocatorCtx;
    private AllocatorGrpc.AllocatorBlockingStub unauthorizedAllocatorBlockingStub;
    private AllocatorGrpc.AllocatorBlockingStub authorizedAllocatorBlockingStub;
    private AllocatorPrivateGrpc.AllocatorPrivateBlockingStub privateAllocatorBlockingStub;
    private OperationServiceApiGrpc.OperationServiceApiBlockingStub operationServiceApiBlockingStub;
    private AllocatorMain allocatorApp;
    private KubernetesServer kubernetesServer;
    private ManagedChannel channel;

    @Before
    public void before() throws IOException {
        super.before();

        kubernetesServer = new KubernetesServer();
        kubernetesServer.before();
        kubernetesServer.expect().post().withPath("/api/v1/namespaces/default/pods")
            .andReturn(HttpURLConnection.HTTP_OK, new PodListBuilder().build())
            .once();

        allocatorCtx = ApplicationContext.run();
        ((MockKuberClientFactory) allocatorCtx.getBean(KuberClientFactory.class)).setClient(
            kubernetesServer.getClient());

        allocatorApp = allocatorCtx.getBean(AllocatorMain.class);
        allocatorApp.start();

        final var config = allocatorCtx.getBean(ServiceConfig.class);
        //noinspection UnstableApiUsage
        channel = ChannelBuilder
            .forAddress(HostAndPort.fromString(config.getAddress()))
            .usePlaintext()
            .build();
        var credentials = config.getIam().createCredentials();
        unauthorizedAllocatorBlockingStub = AllocatorGrpc.newBlockingStub(channel);
        privateAllocatorBlockingStub = AllocatorPrivateGrpc.newBlockingStub(channel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));
        operationServiceApiBlockingStub = OperationServiceApiGrpc.newBlockingStub(channel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));
        authorizedAllocatorBlockingStub = unauthorizedAllocatorBlockingStub.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));
    }

    @After
    public void after() {
        allocatorApp.stop();
        try {
            allocatorApp.awaitTermination();
        } catch (InterruptedException ignored) {
            // ignored
        }

        channel.shutdown();
        try {
            channel.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            //ignored
        }

        allocatorCtx.stop();
        kubernetesServer.after();
        super.after();
    }

    @Test
    public void testUnauthenticated() {
        try {
            //noinspection ResultOfMethodCallIgnored
            unauthorizedAllocatorBlockingStub.createSession(CreateSessionRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
        try {
            //noinspection ResultOfMethodCallIgnored
            unauthorizedAllocatorBlockingStub.deleteSession(DeleteSessionRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
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
                    JwtUtils.invalidCredentials("user")::token));
        try {
            //noinspection ResultOfMethodCallIgnored
            invalidAuthorizedAllocatorBlockingStub.createSession(CreateSessionRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
        try {
            //noinspection ResultOfMethodCallIgnored
            invalidAuthorizedAllocatorBlockingStub.deleteSession(DeleteSessionRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
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
    public void testCreateAndDeleteSession() {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(100).build()).build())
                .build());
        final DeleteSessionResponse deleteSessionResponse = authorizedAllocatorBlockingStub.deleteSession(
            DeleteSessionRequest.newBuilder().setSessionId(createSessionResponse.getSessionId()).build());

        Assert.assertNotNull(createSessionResponse.getSessionId());
        Assert.assertNotNull(deleteSessionResponse);
    }

    @Test
    public void createSessionNoOwner() {
        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.createSession(CreateSessionRequest.newBuilder().setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(100).build()).build())
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void createSessionNoCachePolicy() {
        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.createSession(
                CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void createSessionNoCachePolicyDuration() {
        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.createSession(
                CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().build()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void allocateKuberErrorTest() {
        //simulate kuber api error on pod creation
        kubernetesServer.getKubernetesMockServer().clearExpectations();
        kubernetesServer.expect().post().withPath("/api/v1/namespaces/default/pods")
            .andReturn(HttpURLConnection.HTTP_INTERNAL_ERROR, new PodListBuilder().build())
            .once();

        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(100).build()).build())
                .build());
        final Operation allocate = waitOp(authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build()));
        Assert.assertEquals(Status.INTERNAL.getCode().value(), allocate.getError().getCode());
    }

    @Test
    public void allocateServantTimeoutTest() {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(100).build()).build())
                .build());
        final Operation allocate = waitOp(authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build()));
        Assert.assertEquals(Status.DEADLINE_EXCEEDED.getCode().value(), allocate.getError().getCode());
    }

    @Test
    public void allocateInvalidPoolTest() {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(100).build()).build())
                .build());
        final Operation allocate = waitOp(authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("LABEL")
            .build()));
        Assert.assertEquals(Status.INVALID_ARGUMENT.getCode().value(), allocate.getError().getCode());
    }

    @Test
    public void allocateInvalidSessionTest() {
        try {
            waitOp(authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
                .setSessionId(UUID.randomUUID().toString())
                .setPoolLabel("S")
                .build()));
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }

    }

    @Test
    public void allocateFreeSuccessTest() throws InvalidProtocolBufferException {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(1000).build()).build())
                .build());
        final Operation allocationStarted = authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadata =
            allocationStarted.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);
        registerVm(allocateMetadata.getVmId());

        final Operation allocate = waitOp(allocationStarted);
        final VmAllocatorApi.AllocateResponse allocateResponse =
            allocate.getResponse().unpack(VmAllocatorApi.AllocateResponse.class);
        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());

        Assert.assertTrue(allocate.getDone());
        Assert.assertEquals(createSessionResponse.getSessionId(), allocateResponse.getSessionId());
        Assert.assertEquals("S", allocateResponse.getPoolId());
    }

    @Test
    public void freeNonexistentVmTest() {
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
    public void repeatedFreeTest() throws InvalidProtocolBufferException {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(1000).build()).build())
                .build());
        final Operation allocationStarted = authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadata =
            allocationStarted.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);
        registerVm(allocateMetadata.getVmId());
        waitOp(allocationStarted);
        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());

        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.FAILED_PRECONDITION.getCode(), e.getStatus().getCode());
        }
    }


    @Test
    public void repeatedServantRegister() throws InvalidProtocolBufferException {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(1000).build()).build())
                .build());
        final Operation allocate = authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadata =
            allocate.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);
        registerVm(allocateMetadata.getVmId());

        try {
            //noinspection ResultOfMethodCallIgnored
            privateAllocatorBlockingStub.register(
                VmAllocatorPrivateApi.RegisterRequest.newBuilder().setVmId(allocateMetadata.getVmId()).build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.ALREADY_EXISTS.getCode(),
                e.getStatus().getCode());
        }
    }

    @Test
    public void allocateFromCacheTest() throws InvalidProtocolBufferException {
        final CreateSessionResponse createSessionResponse = authorizedAllocatorBlockingStub.createSession(
            CreateSessionRequest.newBuilder().setOwner(UUID.randomUUID().toString()).setCachePolicy(
                    CachePolicy.newBuilder().setIdleTimeout(Duration.newBuilder().setSeconds(1000).build()).build())
                .build());

        final Operation operationFirst = authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadataFirst =
            operationFirst.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);
        registerVm(allocateMetadataFirst.getVmId());
        waitOp(operationFirst);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(FreeRequest.newBuilder().setVmId(allocateMetadataFirst.getVmId()).build());

        final Operation operationSecond = authorizedAllocatorBlockingStub.allocate(AllocateRequest.newBuilder()
            .setSessionId(createSessionResponse.getSessionId())
            .setPoolLabel("S")
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMetadataSecond =
            operationSecond.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);
        waitOp(operationSecond);

        //noinspection ResultOfMethodCallIgnored
        authorizedAllocatorBlockingStub.free(
            FreeRequest.newBuilder().setVmId(allocateMetadataSecond.getVmId()).build());

        Assert.assertEquals(allocateMetadataFirst.getVmId(), allocateMetadataSecond.getVmId());
    }

    private void registerVm(String vmId) {
        TimeUtils.waitFlagUp(() -> {
            try {
                //noinspection ResultOfMethodCallIgnored
                privateAllocatorBlockingStub.register(
                    VmAllocatorPrivateApi.RegisterRequest.newBuilder().setVmId(vmId).build());
                return true;
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.FAILED_PRECONDITION) {
                    return false;
                }
                throw new RuntimeException(e);
            }
        }, TIMEOUT_SEC, TimeUnit.SECONDS);
    }

    private Operation waitOp(Operation operation) {
        TimeUtils.waitFlagUp(() -> {
            final Operation op = operationServiceApiBlockingStub.get(
                GetOperationRequest.newBuilder().setOperationId(operation.getId()).build());
            return op.getDone();
        }, TIMEOUT_SEC, TimeUnit.SECONDS);
        return operationServiceApiBlockingStub.get(
            GetOperationRequest.newBuilder().setOperationId(operation.getId()).build());
    }
}
