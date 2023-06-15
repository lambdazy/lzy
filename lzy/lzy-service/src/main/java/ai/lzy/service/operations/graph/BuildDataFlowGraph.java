package ai.lzy.service.operations.graph;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.DataFlowGraph;
import ai.lzy.service.dao.ExecuteGraphState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import io.grpc.Status;

import java.util.function.Supplier;

public final class BuildDataFlowGraph extends ExecuteGraphContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    public BuildDataFlowGraph(ExecutionStepContext stepCtx, ExecuteGraphState state) {
        super(stepCtx, state);
    }

    @Override
    public StepResult get() {
        if (dataFlowGraph() != null) {
            log().debug("{} Dataflow graph already built, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Building dataflow graph: { wfName: {}, execId: {} }", logPrefix(), wfName(), execId());

        var dataflowGraph = new DataFlowGraph(operationsToExecute());
        if (dataflowGraph.hasCycle()) {
            log().error("{} Cycle detected in graph", logPrefix());
            return failAction().apply(Status.INVALID_ARGUMENT.withDescription("Cycle detected: " +
                dataflowGraph.printCycle()).asRuntimeException());
        }

        log().debug("{} Save dataflow graph in dao...", logPrefix());
        setDataFlowGraph(dataflowGraph);

        try {
            saveState();
        } catch (Exception e) {
            return retryableFail(e, "Cannot save data about dataflow graph in dao", Status.INTERNAL
                .withDescription("Cannot build graph").asRuntimeException());
        }

        return StepResult.CONTINUE;
    }
}
