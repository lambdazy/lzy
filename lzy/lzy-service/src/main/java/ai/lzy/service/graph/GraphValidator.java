package ai.lzy.service.graph;

import ai.lzy.allocator.vmpool.VmPoolClient;
import ai.lzy.storage.StorageClient;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.workflow.LWF;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Set;

class GraphValidator {
    private static final Logger LOG = LogManager.getLogger(GraphValidator.class);

    private final VmPoolServiceGrpc.VmPoolServiceBlockingStub vmPoolClient;

    public GraphValidator(VmPoolServiceGrpc.VmPoolServiceBlockingStub vmPoolClient) {
        this.vmPoolClient = vmPoolClient;
    }

    public void validate(GraphExecutionState state, StorageClient storageClient) {
        Collection<LWF.Operation> operations = state.getOperations();

        if (operations.isEmpty()) {
            state.fail(Status.INVALID_ARGUMENT, "Collection of graph operations is empty");
            return;
        }

        var dataflowGraph = new DataFlowGraph(operations, storageClient);
        state.setDataFlowGraph(dataflowGraph);
        state.setOperations(dataflowGraph.getOperations());

        if (dataflowGraph.hasCycle()) {
            state.fail(Status.INVALID_ARGUMENT, "Try to execute graph with cycle: " + dataflowGraph.printCycle());
            return;
        }

        var requiredPoolLabels = operations.stream().map(LWF.Operation::getPoolSpecName).toList();
        Set<String> suitableZones;

        try {
            suitableZones = VmPoolClient.findZones(requiredPoolLabels, vmPoolClient);
        } catch (StatusRuntimeException e) {
            state.fail(e.getStatus(), "Cannot obtain vm pools for { poolLabels: %s } , error: %s"
                .formatted(JsonUtils.printAsArray(requiredPoolLabels), e.getStatus().getDescription()));
            return;
        }

        var zone = state.getZone().isBlank() ? suitableZones.stream().findAny().orElse(null) : state.getZone();

        if (zone == null) {
            state.fail(Status.INVALID_ARGUMENT, "Cannot find zone which has all required pools: " +
                JsonUtils.printAsArray(requiredPoolLabels));
            return;
        }

        state.setZone(zone);

        if (!suitableZones.contains(zone)) {
            state.fail(Status.INVALID_ARGUMENT, "Passed zone does not contain all required pools");
        }
    }
}
