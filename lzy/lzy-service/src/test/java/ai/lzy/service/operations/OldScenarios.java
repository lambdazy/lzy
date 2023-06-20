package ai.lzy.service.operations;

import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc.LzyWorkflowPrivateServiceBlockingStub;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;

import java.util.Iterator;
import java.util.function.Consumer;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;


public class OldScenarios {
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

    private static final String workflowName = "test-workflow";
    private static final String reason = "no-matter";

    public OldScenarios(LzyWorkflowServiceBlockingStub lzyGrpcClient,
                        LzyWorkflowPrivateServiceBlockingStub privateLzyGrpcClient)
    {
        this.lzyGrpcClient = lzyGrpcClient;
        this.privateLzyGrpcClient = privateLzyGrpcClient;
    }

    public void startAndFinishExecution() {
        var defaultStorage = lzyGrpcClient
            .getOrCreateDefaultStorage(LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build())
            .getStorage();

        assertBeforeStartWorkflow.run();
        var startWfRequest = LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName)
            .setSnapshotStorage(defaultStorage).build();
        var executionId = withIdempotencyKey(lzyGrpcClient, "start_wf_" + workflowName)
            .startWorkflow(startWfRequest).getExecutionId();
        assertAfterStartWorkflow.run();

        assertBeforeStopWorkflow.run();
        //noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(lzyGrpcClient, "finish_wf_" + workflowName).finishWorkflow(
            LWFS.FinishWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setReason(reason)
                .build());
        assertAfterStopWorkflow.run();
    }

    public void activeExecutionsClash() {
        var defaultStorage = lzyGrpcClient
            .getOrCreateDefaultStorage(LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build())
            .getStorage();

        assertBeforeStartWorkflow.run();
        var startWfRequest = LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName)
            .setSnapshotStorage(defaultStorage).build();
        withIdempotencyKey(lzyGrpcClient, "start_wf_1_" + workflowName).startWorkflow(startWfRequest).getExecutionId();
        assertAfterStartWorkflow.run();

        var secondExecutionId = withIdempotencyKey(lzyGrpcClient, "start_wf_2_" + workflowName)
            .startWorkflow(startWfRequest).getExecutionId();

        assertBeforeStopWorkflow.run();
        //noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(lzyGrpcClient, "finish_wf_2_" + workflowName).finishWorkflow(
            LWFS.FinishWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(secondExecutionId)
                .setReason(reason)
                .build());
        assertAfterStopWorkflow.run();
    }

    public void startAndAbortExecution() {
        var defaultStorage = lzyGrpcClient
            .getOrCreateDefaultStorage(LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build())
            .getStorage();

        assertBeforeStartWorkflow.run();
        var startWfRequest = LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName)
            .setSnapshotStorage(defaultStorage).build();
        var executionId = withIdempotencyKey(lzyGrpcClient, "start_wf_" + workflowName)
            .startWorkflow(startWfRequest).getExecutionId();
        assertAfterStartWorkflow.run();

        assertBeforeStopWorkflow.run();
        //noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(lzyGrpcClient, "abort_wf_" + workflowName).abortWorkflow(
            LWFS.AbortWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setReason(reason)
                .build());
        assertAfterStopWorkflow.run();
    }

    public void startAndPrivateAbortExecution() {
        var defaultStorage = lzyGrpcClient
            .getOrCreateDefaultStorage(LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build())
            .getStorage();

        assertBeforeStartWorkflow.run();
        var startWfRequest = LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName)
            .setSnapshotStorage(defaultStorage).build();
        var executionId = withIdempotencyKey(lzyGrpcClient, "start_wf_" + workflowName)
            .startWorkflow(startWfRequest).getExecutionId();
        assertAfterStartWorkflow.run();

        assertBeforeStopWorkflow.run();
        //noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(privateLzyGrpcClient, "abort_wf_" + workflowName).abortWorkflow(
            LWFS.AbortWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setReason(reason)
                .build());
        assertAfterStopWorkflow.run();
    }

    public OldScenarios setAssertBeforeStartWorkflow(Runnable assertBeforeStartWorkflow) {
        this.assertBeforeStartWorkflow = assertBeforeStartWorkflow;
        return this;
    }

    public OldScenarios setAssertAfterStartWorkflow(Runnable assertAfterStartWorkflow) {
        this.assertAfterStartWorkflow = assertAfterStartWorkflow;
        return this;
    }

    public OldScenarios setAssertBeforeExecuteGraph(Runnable assertBeforeExecuteGraph) {
        this.assertBeforeExecuteGraph = assertBeforeExecuteGraph;
        return this;
    }

    public OldScenarios setAssertAfterExecuteGraph(Runnable assertAfterExecuteGraph) {
        this.assertAfterExecuteGraph = assertAfterExecuteGraph;
        return this;
    }

    public OldScenarios setAssertBeforeStopWorkflow(Runnable assertBeforeStopWorkflow) {
        this.assertBeforeStopWorkflow = assertBeforeStopWorkflow;
        return this;
    }

    public OldScenarios setAssertAfterStopWorkflow(Runnable assertAfterStopWorkflow) {
        this.assertAfterStopWorkflow = assertAfterStopWorkflow;
        return this;
    }

    public OldScenarios setStdReader(Consumer<Iterator<LWFS.ReadStdSlotsResponse>> stdReader) {
        this.stdReader = stdReader;
        return this;
    }
}
