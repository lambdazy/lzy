package ai.lzy.service.operations;

import ai.lzy.service.Graphs;
import ai.lzy.v1.workflow.LWFPS;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc.LzyWorkflowPrivateServiceBlockingStub;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import org.junit.Assert;

import java.util.Iterator;
import java.util.function.Consumer;

public class BaseScenarios {
    private static final Runnable emptyAction = () -> {};

    private Runnable assertBeforeStartWorkflow = emptyAction;
    private Runnable assertAfterStartWorkflow = emptyAction;
    private Runnable assertBeforeExecuteGraph = emptyAction;
    private Runnable assertAfterExecuteGraph = emptyAction;
    private Runnable assertBeforeStopWorkflow = emptyAction;
    private Runnable assertAfterStopWorkflow = emptyAction;
    private Consumer<Iterator<LWFS.ReadStdSlotsResponse>> stdReader = iter -> {};

    private final LzyWorkflowServiceBlockingStub lzyGrpcClient;
    private final LzyWorkflowPrivateServiceBlockingStub privateLzyGrpcClient;

    public BaseScenarios(LzyWorkflowServiceBlockingStub lzyGrpcClient,
                         LzyWorkflowPrivateServiceBlockingStub privateLzyGrpcClient)
    {
        this.lzyGrpcClient = lzyGrpcClient;
        this.privateLzyGrpcClient = privateLzyGrpcClient;
    }

    public void runUsualExecution() {
        var defaultStorage = lzyGrpcClient
            .getOrCreateDefaultStorage(LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build())
            .getStorage();

        assertBeforeStartWorkflow.run();
        var workflowName = "test-workflow";
        var startWfRequest = LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName)
            .setSnapshotStorage(defaultStorage).build();
        var executionId = lzyGrpcClient.startWorkflow(startWfRequest).getExecutionId();
        assertAfterStartWorkflow.run();

        stdReader.accept(
            lzyGrpcClient.readStdSlots(LWFS.ReadStdSlotsRequest.newBuilder().setExecutionId(executionId).build())
        );

        var vmPools = lzyGrpcClient.getAvailablePools(
            LWFS.GetAvailablePoolsRequest.newBuilder()
                .setExecutionId(executionId)
                .build());

        var graph = Graphs.simpleGraph(defaultStorage);

        assertBeforeExecuteGraph.run();
        var graphId = lzyGrpcClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(graph)
                .build()).getGraphId();
        assertAfterExecuteGraph.run();

        boolean processing = true;
        while (processing) {
            var status = lzyGrpcClient.graphStatus(
                LWFS.GraphStatusRequest.newBuilder()
                    .setExecutionId(executionId)
                    .setGraphId(graphId)
                    .build());
            switch (status.getStatusCase()) {
                case WAITING, EXECUTING -> {
                }
                case COMPLETED, FAILED -> processing = false;
                default -> Assert.fail("unexpected graph status");
            }
        }

        assertBeforeStopWorkflow.run();
        //noinspection ResultOfMethodCallIgnored
        lzyGrpcClient.finishWorkflow(
            LWFS.FinishWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .build());
        assertAfterStopWorkflow.run();
    }

    public void runExecutionWithFailedTask() {
        var defaultStorage = lzyGrpcClient
            .getOrCreateDefaultStorage(LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build())
            .getStorage();

        assertBeforeStartWorkflow.run();
        var workflowName = "test-workflow";
        var startWfRequest = LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName)
            .setSnapshotStorage(defaultStorage).build();
        var executionId = lzyGrpcClient.startWorkflow(startWfRequest).getExecutionId();
        assertAfterStartWorkflow.run();

        stdReader.accept(
            lzyGrpcClient.readStdSlots(LWFS.ReadStdSlotsRequest.newBuilder().setExecutionId(executionId).build())
        );

        var vmPools = lzyGrpcClient.getAvailablePools(
            LWFS.GetAvailablePoolsRequest.newBuilder()
                .setExecutionId(executionId)
                .build());

        var graph = Graphs.withMissingOutputSlot(defaultStorage);

        assertBeforeExecuteGraph.run();
        var graphId = lzyGrpcClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(graph)
                .build()).getGraphId();
        assertAfterExecuteGraph.run();

        boolean processing = true;
        while (processing) {
            var status = lzyGrpcClient.graphStatus(
                LWFS.GraphStatusRequest.newBuilder()
                    .setExecutionId(executionId)
                    .setGraphId(graphId)
                    .build());
            switch (status.getStatusCase()) {
                case WAITING, EXECUTING -> {
                }
                case COMPLETED, FAILED -> processing = false;
                default -> Assert.fail("unexpected graph status");
            }
        }

        assertBeforeStopWorkflow.run();
        //noinspection ResultOfMethodCallIgnored
        privateLzyGrpcClient.abortExecution(
            LWFPS.AbortExecutionRequest.newBuilder()
                .setExecutionId(executionId)
                .build());
        assertAfterStopWorkflow.run();
    }

    public void interruptExecutionOnStart() {
        var defaultStorage = lzyGrpcClient
            .getOrCreateDefaultStorage(LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build())
            .getStorage();

        assertBeforeStartWorkflow.run();
        var workflowName = "test-workflow";
        var startWfRequest = LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName)
            .setSnapshotStorage(defaultStorage).build();
        var executionId = lzyGrpcClient.startWorkflow(startWfRequest).getExecutionId();
        assertAfterStartWorkflow.run();

        stdReader.accept(
            lzyGrpcClient.readStdSlots(LWFS.ReadStdSlotsRequest.newBuilder().setExecutionId(executionId).build())
        );

        assertBeforeStopWorkflow.run();
        //noinspection ResultOfMethodCallIgnored
        lzyGrpcClient.abortWorkflow(
            LWFS.AbortWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .build());
        assertAfterStopWorkflow.run();
    }

    public void interruptExecutionWithActiveGraph() {
        var defaultStorage = lzyGrpcClient
            .getOrCreateDefaultStorage(LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build())
            .getStorage();

        assertBeforeStartWorkflow.run();
        var workflowName = "test-workflow";
        var startWfRequest = LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName)
            .setSnapshotStorage(defaultStorage).build();
        var executionId = lzyGrpcClient.startWorkflow(startWfRequest).getExecutionId();
        assertAfterStartWorkflow.run();

        stdReader.accept(
            lzyGrpcClient.readStdSlots(LWFS.ReadStdSlotsRequest.newBuilder().setExecutionId(executionId).build())
        );

        var vmPools = lzyGrpcClient.getAvailablePools(
            LWFS.GetAvailablePoolsRequest.newBuilder()
                .setExecutionId(executionId)
                .build());

        var graph = Graphs.invalidZoneGraph(defaultStorage);

        assertBeforeExecuteGraph.run();
        var graphId = lzyGrpcClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(graph)
                .build()).getGraphId();
        assertAfterExecuteGraph.run();

        assertBeforeStopWorkflow.run();
        //noinspection ResultOfMethodCallIgnored
        lzyGrpcClient.abortWorkflow(
            LWFS.AbortWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .build());
        assertAfterStopWorkflow.run();
    }

    public BaseScenarios setAssertBeforeStartWorkflow(Runnable assertBeforeStartWorkflow) {
        this.assertBeforeStartWorkflow = assertBeforeStartWorkflow;
        return this;
    }

    public BaseScenarios setAssertAfterStartWorkflow(Runnable assertAfterStartWorkflow) {
        this.assertAfterStartWorkflow = assertAfterStartWorkflow;
        return this;
    }

    public BaseScenarios setAssertBeforeExecuteGraph(Runnable assertBeforeExecuteGraph) {
        this.assertBeforeExecuteGraph = assertBeforeExecuteGraph;
        return this;
    }

    public BaseScenarios setAssertAfterExecuteGraph(Runnable assertAfterExecuteGraph) {
        this.assertAfterExecuteGraph = assertAfterExecuteGraph;
        return this;
    }

    public BaseScenarios setAssertBeforeStopWorkflow(Runnable assertBeforeStopWorkflow) {
        this.assertBeforeStopWorkflow = assertBeforeStopWorkflow;
        return this;
    }

    public BaseScenarios setAssertAfterStopWorkflow(Runnable assertAfterStopWorkflow) {
        this.assertAfterStopWorkflow = assertAfterStopWorkflow;
        return this;
    }

    public BaseScenarios setStdReader(Consumer<Iterator<LWFS.ReadStdSlotsResponse>> stdReader) {
        this.stdReader = stdReader;
        return this;
    }
}
