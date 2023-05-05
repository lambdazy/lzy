package ai.lzy.service.graph.execute;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.graph.DataFlowGraph;
import ai.lzy.v1.workflow.LWF;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class BuildDataFlowGraph implements Supplier<StepResult> {
    private final String wfName;
    private final String execId;
    private final Collection<LWF.Operation> operations;
    private final Consumer<DataFlowGraph> resultConsumer;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public BuildDataFlowGraph(String wfName, String execId, Collection<LWF.Operation> operations,
                              Consumer<DataFlowGraph> resultConsumer,
                              Function<StatusRuntimeException, StepResult> failAction,
                              Logger log, String logPrefix)
    {
        this.wfName = wfName;
        this.execId = execId;
        this.operations = operations;
        this.resultConsumer = resultConsumer;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        log.info("{} Building dataflow graph: { wfName: {}, execId: {} }", logPrefix, wfName, execId);
        var dataflowGraph = new DataFlowGraph(operations);
        if (dataflowGraph.hasCycle()) {
            log.error("{} Cycle detected in graph", logPrefix);
            return failAction.apply(Status.INVALID_ARGUMENT.withDescription("Cycle detected: " +
                dataflowGraph.printCycle()).asRuntimeException());
        }

        resultConsumer.accept(dataflowGraph);
        return StepResult.CONTINUE;
    }
}
