package ai.lzy.service;

import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.test.TimeUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.portal.LzyPortalGrpc;
import ai.lzy.v1.workflow.LWFS;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class WorkflowTest extends BaseTest {
    @Test
    public void createWorkflow() {
        authorizedWorkflowClient.createWorkflow(
            LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build());
        var thrown = Assert.assertThrows(StatusRuntimeException.class, () -> authorizedWorkflowClient
            .createWorkflow(LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build()));

        var expectedStatusCode = Status.ALREADY_EXISTS.getCode();

        Assert.assertEquals(expectedStatusCode, thrown.getStatus().getCode());
    }

    @Test
    public void tempBucketCreationFailed() {
        storageServer.shutdown();

        var thrown = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.createWorkflow(
                LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build()));

        var expectedErrorCode = Status.UNAVAILABLE.getCode();

        Assert.assertEquals(expectedErrorCode, thrown.getStatus().getCode());
    }

    @Test
    public void createWorkflowFailedWithUserStorageMissedEndpoint() {
        var thrown = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.createWorkflow(LWFS.CreateWorkflowRequest.newBuilder()
                .setWorkflowName("workflow_1")
                .setSnapshotStorage(LMS3.S3Locator.newBuilder()
                    .setKey("some-valid-key")
                    .setBucket("some-valid-bucket")
                    .build())
                .build()));

        var expectedErrorCode = Status.INVALID_ARGUMENT.getCode();

        Assert.assertEquals(expectedErrorCode, thrown.getStatus().getCode());
    }

    @Test
    public void tempBucketDeletedIfCreateExecutionFailed() {
        Assert.assertThrows(StatusRuntimeException.class, () -> {
            authorizedWorkflowClient.createWorkflow(
                LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build());
            authorizedWorkflowClient.createWorkflow(
                LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build());
        });

        TimeUtils.waitFlagUp(() -> storageMock.getBuckets().size() == 1, 300, TimeUnit.SECONDS);

        var expectedBucketCount = 1;
        var actualBucketCount = storageMock.getBuckets().size();

        Assert.assertEquals(expectedBucketCount, actualBucketCount);
    }

    @Test
    public void finishWorkflow() {
        var executionId = authorizedWorkflowClient.createWorkflow(
            LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_2").build()
        ).getExecutionId();

        authorizedWorkflowClient.finishWorkflow(
            LWFS.FinishWorkflowRequest.newBuilder()
                .setWorkflowName("workflow_2")
                .setExecutionId(executionId)
                .build());

        var thrownAlreadyFinished = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.finishWorkflow(LWFS.FinishWorkflowRequest.newBuilder()
                .setWorkflowName("workflow_2")
                .setExecutionId(executionId)
                .build()));

        var thrownUnknownWorkflow = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.finishWorkflow(
                LWFS.FinishWorkflowRequest.newBuilder()
                    .setWorkflowName("workflow_3")
                    .setExecutionId("execution_id")
                    .build()));

        Assert.assertEquals(Status.INTERNAL.getCode(), thrownAlreadyFinished.getStatus().getCode());
        Assert.assertEquals(Status.INTERNAL.getCode(), thrownUnknownWorkflow.getStatus().getCode());
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testPortalStartedWhileCreatingWorkflow() {
        authorizedWorkflowClient.createWorkflow(
            LWFS.CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build());

        var config = context.getBean(LzyServiceConfig.class);
        var internalUserCredentials = config.getIam().createCredentials();
        var portalAddress = HostAndPort.fromParts("localhost", config.getPortal().getPortalApiPort());
        var portalChannel = ChannelBuilder.forAddress(portalAddress).usePlaintext().build();
        var portalClient = LzyPortalGrpc.newBlockingStub(portalChannel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, internalUserCredentials::token));

        portalClient.status(Empty.getDefaultInstance());
    }
}
