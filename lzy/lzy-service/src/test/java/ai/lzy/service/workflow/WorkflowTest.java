package ai.lzy.service.workflow;

import ai.lzy.service.BaseTest;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalGrpc;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import com.google.common.net.HostAndPort;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static org.junit.Assert.*;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class WorkflowTest extends BaseTest {
    @Test
    public void tempBucketCreationFailed() throws InterruptedException {
        shutdownStorage();

        var thrown = assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.startExecution(
                LWFS.StartExecutionRequest.newBuilder().setWorkflowName("workflow_1").build()));

        var expectedErrorCode = Status.UNAVAILABLE.getCode();

        assertEquals(expectedErrorCode, thrown.getStatus().getCode());
    }

    @Test
    public void startExecutionFailedWithUserStorageMissedCredentials() {
        var thrown = assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.startExecution(LWFS.StartExecutionRequest.newBuilder()
                .setWorkflowName("workflow_1")
                .setSnapshotStorage(LMST.StorageConfig.newBuilder()
                    .setUri("s3://bucket/key")
                    .build())
                .build()));

        var expectedErrorCode = Status.INVALID_ARGUMENT.getCode();

        assertEquals(expectedErrorCode, thrown.getStatus().getCode());
    }

    @Test
    public void startAndFinishExecutionWithInternalStorage() {
        var executionId = authorizedWorkflowClient.startExecution(
            LWFS.StartExecutionRequest.newBuilder().setWorkflowName("workflow_2").build()
        ).getExecutionId();

        String[] destroyedExecutionChannels = {null};
        onChannelsDestroy(exId -> destroyedExecutionChannels[0] = exId);

        var deleteSessionFlag = new AtomicBoolean(false);
        onDeleteSession(() -> deleteSessionFlag.set(true));

        var freeVmFlag = new AtomicBoolean(false);
        onFreeVm(() -> freeVmFlag.set(true));

        var finishOp = authorizedWorkflowClient.finishExecution(
            LWFS.FinishExecutionRequest.newBuilder()
                .setExecutionId(executionId)
                .build());
        finishOp = awaitOperationDone(operationServiceClient, finishOp.getId(), Duration.ofSeconds(5));

        assertTrue(finishOp.getDone());
        assertTrue(finishOp.hasResponse());

        // assertEquals(executionId, destroyedExecutionChannels[0]);
        assertTrue(deleteSessionFlag.get());
        assertTrue(freeVmFlag.get());

        var thrownAlreadyFinished = assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.finishExecution(LWFS.FinishExecutionRequest.newBuilder()
                .setExecutionId(executionId)
                .build()));

        var thrownUnknownWorkflow = assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.finishExecution(
                LWFS.FinishExecutionRequest.newBuilder()
                    .setExecutionId("execution_id")
                    .build()));

        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrownAlreadyFinished.getStatus().getCode());
        assertEquals(Status.NOT_FOUND.getCode(), thrownUnknownWorkflow.getStatus().getCode());
    }

    @Test
    public void startAndFinishExecutionWithGraph() {
        var execution = authorizedWorkflowClient.startExecution(
            LWFS.StartExecutionRequest.newBuilder().setWorkflowName("workflow_2").build()
        );

        var executionId = execution.getExecutionId();
        var storageConfig = execution.getInternalSnapshotStorage();

        var operations = List.of(
            LWF.Operation.newBuilder()
                .setName("first task prints string 'i-am-hacker' to variable")
                .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_worker_1/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_1/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .setPoolSpecName("s")
                .build(),
            LWF.Operation.newBuilder()
                .setName("second task reads string 'i-am-hacker' from variable and prints it to another one")
                .setCommand("/tmp/lzy_worker_2/sbin/cat /tmp/lzy_worker_2/a > /tmp/lzy_worker_2/b")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_2/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_2/b")
                    .setStorageUri(buildSlotUri("snapshot_b_1", storageConfig))
                    .build())
                .setPoolSpecName("s")
                .build()
        );

        var graph = LWF.Graph.newBuilder()
            .setName("simple-graph")
            .setZone("ru-central1-a")
            .addAllOperations(operations)
            .build();

        var expectedGraphId = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setExecutionId(executionId).setGraph(graph).build()).getGraphId();

        String[] destroyedExecutionChannels = {null};
        onChannelsDestroy(exId -> destroyedExecutionChannels[0] = exId);

        var deleteSessionFlag = new AtomicBoolean(false);
        onDeleteSession(() -> deleteSessionFlag.set(true));

        var freeVmFlag = new AtomicBoolean(false);
        onFreeVm(() -> freeVmFlag.set(true));

        String[] stopGraphId = {null};
        onStopGraph(actualGraphId -> stopGraphId[0] = actualGraphId);

        var finishOp = authorizedWorkflowClient.finishExecution(
            LWFS.FinishExecutionRequest.newBuilder()
                .setExecutionId(executionId)
                .build());
        finishOp = awaitOperationDone(operationServiceClient, finishOp.getId(), Duration.ofSeconds(15));

        assertTrue(finishOp.getDone());
        assertTrue(finishOp.hasResponse());

        //assertEquals(executionId, destroyedExecutionChannels[0]);
        assertTrue(deleteSessionFlag.get());
        assertTrue(freeVmFlag.get());
        assertEquals(expectedGraphId, stopGraphId[0]);
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
