package ai.lzy.service.graph;

import ai.lzy.service.Graphs;
import ai.lzy.service.ContextAwareTests;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.workflow.LWF.Graph;
import ai.lzy.v1.workflow.LWFS;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

public abstract class AbstractGraphExecutionTest extends ContextAwareTests {
    protected LMST.StorageConfig storageConfig;

    @Before
    public void setUp() throws IOException, InterruptedException {
        storageConfig = authLzyGrpcClient.getOrCreateDefaultStorage(
            LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build()).getStorage();
    }

    protected LWFS.StartWorkflowResponse startWorkflow(String workflowName) {
        var request = LWFS.StartWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setSnapshotStorage(storageConfig)
            .build();
        return authLzyGrpcClient.startWorkflow(request);
    }

    protected void finishWorkflow(String workflowName, String executionId) {
        var request = LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setReason("no-matter")
            .build();
        //noinspection ResultOfMethodCallIgnored
        authLzyGrpcClient.finishWorkflow(request);
    }

    protected Graph emptyGraph() {
        return Graph.newBuilder().setName("empty-graph").build();
    }

    protected Graph simpleGraph() {
        return Graphs.simpleGraph(storageConfig);
    }

    protected Graph cyclicGraph() {
        return Graphs.cyclicGraph(storageConfig);
    }

    protected Graph nonSuitableZoneGraph() {
        return Graphs.nonSuitableZoneGraph(storageConfig);
    }

    protected Graph invalidZoneGraph() {
        return Graphs.invalidZoneGraph(storageConfig);
    }

    protected Graph unknownSlotUriGraph() {
        return Graphs.unknownSlotUriGraph(storageConfig);
    }

    protected Graph withMissingOutputSlot() {
        return Graphs.withMissingOutputSlot(storageConfig);
    }

    protected Graph graphWithRepeatedOps() {
        return Graphs.graphWithRepeatedOps(storageConfig);
    }

    protected List<Graph> producerAndConsumersGraphs() {
        return Graphs.producerAndConsumersGraphs(storageConfig);
    }
}
