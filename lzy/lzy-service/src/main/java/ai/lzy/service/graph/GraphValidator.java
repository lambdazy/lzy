package ai.lzy.service.graph;

import ai.lzy.service.dao.ExecuteGraphState;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.workflow.LWF;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.util.Strings;

import java.util.Set;

import static ai.lzy.allocator.vmpool.VmPoolClient.findZones;

class GraphValidator {
    private final VmPoolServiceGrpc.VmPoolServiceBlockingStub vmPoolClient;

    public GraphValidator(VmPoolServiceGrpc.VmPoolServiceBlockingStub vmPoolClient) {
        this.vmPoolClient = vmPoolClient;
    }

    public void validate(ExecuteGraphState state) {
        if (!validateZone(state)) {
            return;
        }

        state.setDataFlowGraph(new DataFlowGraph(state.getCacheProcessedOperations()));
        state.setCacheProcessedOperations(state.getDataFlowGraph().getOperations());

        if (state.getDataFlowGraph().hasCycle()) {
            state.fail(Status.INVALID_ARGUMENT, "Cycle detected: " + state.getDataFlowGraph().printCycle());
        }
    }

    private boolean validateZone(ExecuteGraphState state) {
        var requiredPoolLabels = state.getCacheProcessedOperations().stream().map(LWF.Operation::getPoolSpecName).toList();
        Set<String> suitableZones;
        try {
            suitableZones = findZones(requiredPoolLabels, vmPoolClient);
        } catch (StatusRuntimeException e) {
            state.fail(e.getStatus(), "Cannot obtain vm pools for { poolLabels: %s, error: %s }"
                .formatted(JsonUtils.printAsArray(requiredPoolLabels), e.getStatus().getDescription()));
            return false;
        }

        var requestedZone = state.getVmPoolZone();
        if (Strings.isBlank(requestedZone)) {
            suitableZones.stream().findAny().ifPresentOrElse(state::setVmPoolZone, () -> state.fail(Status.INVALID_ARGUMENT,
                "Cannot find zone which has all required pools: " + JsonUtils.printAsArray(requiredPoolLabels)));
        } else if (!suitableZones.contains(requestedZone)) {
            state.setVmPoolZone(null);
            state.fail(Status.INVALID_ARGUMENT, "Passed zone'" + requestedZone + "' doesn't contain required pools");
        }

        return state.getVmPoolZone() != null;
    }
}
