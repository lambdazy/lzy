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
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static org.junit.Assert.*;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class WorkflowTest extends BaseTest {
    @Test
    public void tempBucketCreationFailed() throws InterruptedException {
        shutdownStorage();

        var thrown = assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.startWorkflow(
                LWFS.StartWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build()));

        var expectedErrorCode = Status.UNAVAILABLE.getCode();

        assertEquals(expectedErrorCode, thrown.getStatus().getCode());
    }

    @Test
    public void startExecutionFailedWithUserStorageMissedCredentials() {
        var thrown = assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
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
        var workflowName = "workflow_2";
        var executionId = authorizedWorkflowClient.startWorkflow(
            LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName).build()
        ).getExecutionId();

        String[] destroyedExecutionChannels = {null};
        onChannelsDestroy(exId -> destroyedExecutionChannels[0] = exId);

        var deleteSessionFlag = new AtomicBoolean(false);
        onDeleteSession(() -> deleteSessionFlag.set(true));

        var freeVmFlag = new AtomicBoolean(false);
        onFreeVm(() -> freeVmFlag.set(true));

        var finishOp = authorizedWorkflowClient.finishWorkflow(
            LWFS.FinishWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .build());
        finishOp = awaitOperationDone(operationServiceClient, finishOp.getId(), Duration.ofSeconds(5));

        assertTrue(finishOp.getDone());
        assertTrue(finishOp.hasResponse());

        assertEquals(executionId, destroyedExecutionChannels[0]);
        assertTrue(deleteSessionFlag.get());
        assertTrue(freeVmFlag.get());

        var thrownAlreadyFinished = assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.finishWorkflow(LWFS.FinishWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .build()));

        var thrownUnknownWorkflowExecution = assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.finishWorkflow(
                LWFS.FinishWorkflowRequest.newBuilder()
                    .setWorkflowName(workflowName)
                    .setExecutionId("execution_id")
                    .build()));

        assertEquals(Status.FAILED_PRECONDITION.getCode(), thrownAlreadyFinished.getStatus().getCode());
        assertEquals(Status.FAILED_PRECONDITION.getCode(), thrownUnknownWorkflowExecution.getStatus().getCode());
    }

    @Test
    public void startAndFinishExecutionWithGraph() {
        var workflowName = "workflow_2";
        var execution = authorizedWorkflowClient.startWorkflow(
            LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName).build()
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

        var finishOp = authorizedWorkflowClient.finishWorkflow(
            LWFS.FinishWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .build());
        finishOp = awaitOperationDone(operationServiceClient, finishOp.getId(), Duration.ofSeconds(15));

        assertTrue(finishOp.getDone());
        assertTrue(finishOp.hasResponse());

        assertEquals(executionId, destroyedExecutionChannels[0]);
        assertTrue(deleteSessionFlag.get());
        assertTrue(freeVmFlag.get());
        assertEquals(expectedGraphId, stopGraphId[0]);
    }

    @Test
    public void startAndAbortWorkflowExecutionWithGraphs() {
        var workflowName = "workflow_1";
        var execution = authorizedWorkflowClient.startWorkflow(
            LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName).build()
        );

        var executionId = execution.getExecutionId();
        var storageConfig = execution.getInternalSnapshotStorage();

        var graphs = IntStream.range(0, 10).boxed().map(i -> {
                var output1 = LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_1/a")
                    .setStorageUri(buildSlotUri("snapshot_a_" + i, storageConfig))
                    .build();
                var input2 = LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_2/a")
                    .setStorageUri(buildSlotUri("snapshot_a_" + i, storageConfig))
                    .build();
                var output2 = LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_2/b")
                    .setStorageUri(buildSlotUri("snapshot_b_" + i, storageConfig))
                    .build();

                var operations = List.of(
                    LWF.Operation.newBuilder()
                        .setName("first task prints string 'i-am-hacker' to variable")
                        .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_worker_1/a")
                        .addOutputSlots(output1)
                        .setPoolSpecName("s")
                        .build(),
                    LWF.Operation.newBuilder()
                        .setName("second task reads string 'i-am-hacker' from variable and prints it to another one")
                        .setCommand("/tmp/lzy_worker_2/sbin/cat /tmp/lzy_worker_2/a > /tmp/lzy_worker_2/b")
                        .addInputSlots(input2)
                        .addOutputSlots(output2)
                        .setPoolSpecName("s")
                        .build()
                );

                return LWF.Graph.newBuilder()
                    .setName("simple-graph-" + i)
                    .setZone("ru-central1-a")
                    .addAllOperations(operations)
                    .build();
            }
        ).toList();

        var graphIds = graphs.stream().map(graph -> authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setExecutionId(executionId).setGraph(graph).build()).getGraphId()
        ).collect(Collectors.toSet());

        var stoppedGraphs = new HashSet<String>(10);
        onStopGraph(stoppedGraphs::add);

        authorizedWorkflowClient.abortWorkflow(LWFS.AbortWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).setExecutionId(executionId).build());

        assertTrue(stoppedGraphs.containsAll(graphIds));
        assertTrue(graphIds.containsAll(stoppedGraphs));
    }

    @Test
    public void failToAbortUnknownWorkflowExecutions() {
        var workflowName = "workflow_1";
        var execution = authorizedWorkflowClient.startWorkflow(
            LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName).build()
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

        var thrown1 = assertThrows(StatusRuntimeException.class, () -> authorizedWorkflowClient.abortWorkflow(
            LWFS.AbortWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId("invalid_execution_id")
                .build()));

        var thrown2 = assertThrows(StatusRuntimeException.class, () -> authorizedWorkflowClient.abortWorkflow(
            LWFS.AbortWorkflowRequest.newBuilder()
                .setWorkflowName("invalid_wf_name")
                .setExecutionId(executionId)
                .build()));

        authorizedWorkflowClient.abortWorkflow(
            LWFS.AbortWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .build());

        assertSame(Status.FAILED_PRECONDITION.getCode(), thrown1.getStatus().getCode());
        assertSame(Status.NOT_FOUND.getCode(), thrown2.getStatus().getCode());
        assertEquals(expectedGraphId, stopGraphId[0]);
    }

    @Test
    public void testPortalStartedWhileCreatingWorkflow() throws InterruptedException {
        WorkflowService.PEEK_RANDOM_PORTAL_PORTS = false;
        var workflowName = "workflow_1";
        var exId = authorizedWorkflowClient.startWorkflow(
            LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName).build()).getExecutionId();

        var portalAddress = HostAndPort.fromParts("localhost", config.getPortal().getPortalApiPort());
        var portalChannel = newGrpcChannel(portalAddress, LzyPortalGrpc.SERVICE_NAME);
        var portalClient = LzyPortalGrpc.newBlockingStub(portalChannel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, () -> internalUserCredentials.get().token()));

        portalClient.status(LzyPortalApi.PortalStatusRequest.newBuilder().build());

        portalChannel.shutdown();
        portalChannel.awaitTermination(5, TimeUnit.SECONDS);

        authorizedWorkflowClient.finishWorkflow(LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).setExecutionId(exId).build());
    }
}
