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
import java.util.Set;
import java.util.stream.Collectors;

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

    public void validate(GraphExecutionState state, LWF.Graph graph) {
        Collection<LWF.Operation> operations = graph.getOperationsList();
        var slotsUriAsOutput = operations.stream()
            .flatMap(op -> op.getOutputSlotsList().stream().map(LWF.Operation.SlotDescription::getStorageUri))
            .collect(Collectors.toSet());
        var duplicate = findFirstDuplicate(slotsUriAsOutput);

        if (duplicate != null) {
            state.onError(Status.INVALID_ARGUMENT, "Duplicated output slot URI: " + duplicate);
            return;
        }

        Set<String> knownSlots;
        try {
            knownSlots = withRetries(LOG, () -> executionDao.retainExistingSlots(slotsUriAsOutput));
        } catch (Exception e) {
            state.onError(Status.INTERNAL, "Cannot obtain existing slots URIs while starting graph: " + e.getMessage());
            return;
        }

        if (!knownSlots.isEmpty()) {
            state.onError(Status.INVALID_ARGUMENT, "Output slots URIs { slotsUri: %s } already used in other execution"
                .formatted(JsonUtils.printAsArray(knownSlots)));
            return;
        }

        var dataflowGraph = new DataFlowGraph(operations);

        state.setDataFlowGraph(dataflowGraph);

        if (dataflowGraph.hasCycle()) {
            state.onError(Status.INVALID_ARGUMENT, "Try to execute graph with cycle: " + dataflowGraph.printCycle());
            return;
        }

        Set<String> unknownSlots;
        Set<String> fromPortal = dataflowGraph.getDanglingInputSlots().keySet();
        try {
            unknownSlots = withRetries(LOG, () ->
                executionDao.retainNonExistingSlots(state.getExecutionId(), fromPortal));
        } catch (Exception e) {
            state.onError(Status.INTERNAL, "Cannot obtain non-existing slots URIs associated with execution: " +
                e.getMessage());
            return;
        }

        if (!unknownSlots.isEmpty()) {
            state.onError(Status.NOT_FOUND, String.format("Slots URIs { slotUris: %s } are presented neither in " +
                "output slots URIs nor stored as already associated with portal",
                JsonUtils.printAsArray(unknownSlots)));
            return;
        }

        var requiredPoolLabels = operations.stream().map(LWF.Operation::getPoolSpecName).toList();
        Set<String> suitableZones;

        try {
            suitableZones = VmPoolClient.findZones(requiredPoolLabels, vmPoolClient);
        } catch (StatusRuntimeException e) {
            state.onError(e.getStatus(), "Cannot obtain vm pools for { poolLabels: %s } , error: %s"
                .formatted(JsonUtils.printAsArray(requiredPoolLabels), e.getStatus().getDescription()));
            return;
        }

        var zoneName = graph.getZone().isBlank() ? suitableZones.stream().findAny().orElse(null) : graph.getZone();

        if (zoneName == null) {
            state.onError(Status.INVALID_ARGUMENT, "Cannot find zone which has all required pools: " +
                JsonUtils.printAsArray(requiredPoolLabels));
            return;
        }

        state.setZoneName(zoneName);

        if (!suitableZones.contains(zoneName)) {
            state.onError(Status.INVALID_ARGUMENT, "Passed zone does not contain all required pools");
        }
    }
}
