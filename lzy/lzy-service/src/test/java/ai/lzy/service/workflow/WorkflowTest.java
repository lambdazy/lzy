package ai.lzy.service.workflow;

import ai.lzy.service.BaseTest;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalGrpc;
import ai.lzy.v1.workflow.LWFS;
import com.google.common.net.HostAndPort;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class WorkflowTest extends BaseTest {
    @Test
    public void tempBucketCreationFailed() throws InterruptedException {
        shutdownStorage();

        var thrown = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.startExecution(
                LWFS.StartExecutionRequest.newBuilder().setWorkflowName("workflow_1").build()));

        var expectedErrorCode = Status.UNAVAILABLE.getCode();

        Assert.assertEquals(expectedErrorCode, thrown.getStatus().getCode());
    }

    @Test
    public void startExecutionFailedWithUserStorageMissedEndpoint() {
        var thrown = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.startExecution(LWFS.StartExecutionRequest.newBuilder()
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
    public void startAndFinishExecution() {
        var executionId = authorizedWorkflowClient.startExecution(
            LWFS.StartExecutionRequest.newBuilder().setWorkflowName("workflow_2").build()
        ).getExecutionId();

        authorizedWorkflowClient.finishExecution(
            LWFS.FinishExecutionRequest.newBuilder()
                .setExecutionId(executionId)
                .build());

        var thrownAlreadyFinished = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.finishExecution(LWFS.FinishExecutionRequest.newBuilder()
                .setExecutionId(executionId)
                .build()));

        var thrownUnknownWorkflow = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.finishExecution(
                LWFS.FinishExecutionRequest.newBuilder()
                    .setExecutionId("execution_id")
                    .build()));

        Assert.assertEquals(Status.INVALID_ARGUMENT.getCode(), thrownAlreadyFinished.getStatus().getCode());
        Assert.assertEquals(Status.NOT_FOUND.getCode(), thrownUnknownWorkflow.getStatus().getCode());
    }

    @Test
    public void testPortalStartedWhileCreatingWorkflow() throws InterruptedException {
        WorkflowService.PEEK_RANDOM_PORTAL_PORTS = false;
        var exId = authorizedWorkflowClient.startExecution(
            LWFS.StartExecutionRequest.newBuilder().setWorkflowName("workflow_1").build()).getExecutionId();

        var portalAddress = HostAndPort.fromParts("localhost", config.getPortal().getPortalApiPort());
        var portalChannel = newGrpcChannel(portalAddress, LzyPortalGrpc.SERVICE_NAME);
        var portalClient = LzyPortalGrpc.newBlockingStub(portalChannel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, () -> internalUserCredentials.get().token()));

        portalClient.status(LzyPortalApi.PortalStatusRequest.newBuilder().build());

        portalChannel.shutdown();
        portalChannel.awaitTermination(5, TimeUnit.SECONDS);

        authorizedWorkflowClient.finishExecution(LWFS.FinishExecutionRequest.newBuilder().setExecutionId(exId).build());
    }
}
