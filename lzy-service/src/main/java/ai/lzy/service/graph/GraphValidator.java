package ai.lzy.service.graph;

import ai.lzy.allocator.vmpool.VmPoolClient;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.workflow.LWF;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.util.CollectionUtils.findFirstDuplicate;

class GraphValidator {
    private static final Logger LOG = LogManager.getLogger(GraphValidator.class);

    private final ExecutionDao executionDao;
    private final VmPoolServiceGrpc.VmPoolServiceBlockingStub vmPoolClient;

    public GraphValidator(ExecutionDao executionDao,
                          VmPoolServiceGrpc.VmPoolServiceBlockingStub vmPoolClient)
    {
        this.executionDao = executionDao;
        this.vmPoolClient = vmPoolClient;
    }

    public void validate(GraphExecutionState state) {
        Collection<LWF.Operation> operations = state.getOperations();

        if (operations.isEmpty()) {
            state.fail(Status.INVALID_ARGUMENT, "Collection of graph operations is empty");
            return;
        }

        var allOutputSlotsUriList = operations.stream()
            .flatMap(op -> op.getOutputSlotsList().stream().map(LWF.Operation.SlotDescription::getStorageUri))
            .toList();
        var duplicate = findFirstDuplicate(allOutputSlotsUriList);

        if (duplicate != null) {
            state.fail(Status.INVALID_ARGUMENT, "Duplicated output slot URI: " + duplicate);
            return;
        }

        Set<String> knownSlots;
        try {
            knownSlots = withRetries(LOG, () -> executionDao.retainExistingSlots(new HashSet<>(allOutputSlotsUriList)));
        } catch (Exception e) {
            state.fail(Status.INTERNAL, "Cannot obtain existing slots URIs while starting graph: " + e.getMessage());
            return;
        }

        if (!knownSlots.isEmpty()) {
            state.fail(Status.INVALID_ARGUMENT, "Output slots URIs { slotsUri: %s } already used in other execution"
                .formatted(JsonUtils.printAsArray(knownSlots)));
            return;
        }

        var nodes = operations.stream()
            .map(op -> new DataFlowGraph.Node(op.getName(), op.getInputSlotsList(), op.getOutputSlotsList()))
            .toList();
        var dataflowGraph = new DataFlowGraph(nodes);

        state.setDataFlowGraph(dataflowGraph);

        if (dataflowGraph.hasCycle()) {
            state.fail(Status.INVALID_ARGUMENT, "Try to execute graph with cycle: " + dataflowGraph.printCycle());
            return;
        }

        Set<String> unknownSlots;
        Set<String> fromPortal = dataflowGraph.getDanglingInputSlots().keySet();
        try {
            unknownSlots = withRetries(LOG, () ->
                executionDao.retainNonExistingSlots(state.getExecutionId(), fromPortal));
        } catch (Exception e) {
            state.fail(Status.INTERNAL, "Cannot obtain non-existing slots URIs associated with execution: " +
                e.getMessage());
            return;
        }

        if (!unknownSlots.isEmpty()) {
            state.fail(Status.NOT_FOUND, String.format("Slots URIs { slotUris: %s } are presented neither in " +
                "output slots URIs nor stored as already associated with portal",
                JsonUtils.printAsArray(unknownSlots)));
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
