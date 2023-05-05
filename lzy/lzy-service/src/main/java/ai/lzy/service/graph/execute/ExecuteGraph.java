package ai.lzy.service.graph.execute;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.v1.graph.GraphExecutorApi.GraphExecuteRequest;
import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.Logger;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class ExecuteGraph implements Supplier<StepResult> {
    private final GraphExecutorBlockingStub graphExecutor;
    private final GraphExecuteRequest.Builder builder;

    private final Consumer<String> graphIdConsumer;
    private final Function<StatusRuntimeException, StepResult> failAction;

    private final Logger log;
    private final String logPrefix;

    public ExecuteGraph(GraphExecutorBlockingStub graphExecutorClient, GraphExecuteRequest.Builder builder,
                        Consumer<String> graphIdConsumer, Function<StatusRuntimeException, StepResult> failAction,
                        Logger log, String logPrefix)
    {
        this.graphExecutor = graphExecutorClient;
        this.builder = builder;
        this.graphIdConsumer = graphIdConsumer;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        log.info("{} Send execute graph request to service", logPrefix);

        String graphId;
        try {
            graphId = graphExecutor.execute(builder.build()).getStatus().getGraphId();
        } catch (StatusRuntimeException sre) {
            log.error("{} Error while GraphExecutor::execute call: {}", logPrefix, sre.getMessage(), sre);
            return failAction.apply(sre);
        }

        log.debug("{} Graph executor service accept execute graph request: { graphId: {} }", logPrefix, graphId);
        graphIdConsumer.accept(graphId);
        return StepResult.CONTINUE;
    }
}
