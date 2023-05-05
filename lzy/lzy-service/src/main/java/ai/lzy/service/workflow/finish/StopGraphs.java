package ai.lzy.service.workflow.finish;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.data.dao.GraphDao;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public class StopGraphs implements Supplier<StepResult> {
    private final GraphDao graphDao;
    private final String execId;
    private final GraphExecutorBlockingStub idempotentGraphExecClient;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public StopGraphs(GraphDao graphDao, String execId,
                      GraphExecutorBlockingStub idempotentGraphExecClient,
                      Function<StatusRuntimeException, StepResult> failAction,
                      Logger log, String logPrefix)
    {
        this.graphDao = graphDao;
        this.execId = execId;
        this.idempotentGraphExecClient = idempotentGraphExecClient;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        log.debug("{} Stop graphs", logPrefix);

        try {
            //noinspection ResultOfMethodCallIgnored
            withRetries(log, () -> graphDao.getAll(execId)).forEach(
                graph -> idempotentGraphExecClient.stop(
                    GraphExecutorApi.GraphStopRequest.newBuilder()
                        .setWorkflowId(graph.executionId())
                        .setGraphId(graph.graphId())
                        .build())
            );
        } catch (StatusRuntimeException sre) {
            log.error("{} Error while GraphExecutorBlockingStub::stopGraph call: {}", logPrefix, sre.getMessage(), sre);
            return failAction.apply(sre);
        } catch (Exception e) {
            log.error("{} Error while getting execution graphs from dao: {}", logPrefix, e.getMessage(), e);
            return failAction.apply(Status.INTERNAL.withDescription("Cannot stop execution graphs")
                .asRuntimeException());
        }

        return StepResult.CONTINUE;
    }
}
