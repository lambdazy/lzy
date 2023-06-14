package ai.lzy.graph;

import ai.lzy.graph.GraphExecutorApi2.*;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.model.DirectedGraph;
import ai.lzy.graph.services.GraphService;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.v1.longrunning.LongRunning;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOp;

@Singleton
public class GraphExecutorApi extends GraphExecutorGrpc.GraphExecutorImplBase {
    private static final Logger LOG = LogManager.getLogger(GraphExecutorApi.class);

    private final GraphService graphService;
    private final OperationDao operationDao;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @Inject
    public GraphExecutorApi(ServiceConfig config,
                            GraphService graphService,
                            @Named("GraphExecutorOperationDao") OperationDao operationDao)
    {
        this.graphService = graphService;
        this.operationDao = operationDao;

        executor.schedule(() -> {
            try {
                operationDao.deleteOutdatedOperations(config.getGcPeriod().toHoursPart());
            } catch (SQLException e) {
                LOG.error("Cannot delete outdated operations");
            }
        }, config.getGcPeriod().getSeconds(), TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }

    @Override
    public void execute(GraphExecuteRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
        if (!validateRequest(request, responseObserver)) {
            return;
        }

        Operation.IdempotencyKey idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationDao, idempotencyKey, responseObserver, LOG)) {
            return;
        }

        final DirectedGraph graph = graphService.buildGraph(request);
        try {
            graphService.validateGraph(graph);
        } catch (Exception e) {
            LOG.error("Graph did not pass validation: { graph: {}, error: {} }", graph, e.getMessage());
            var errorStatus = Status.INVALID_ARGUMENT.withDescription(e.getMessage());
            responseObserver.onError(errorStatus.asException());
            return;
        }

        var op = Operation.create(
            request.getUserId(),
            "Execute graph of execution: executionId='%s'".formatted(request.getWorkflowId()),
            null,
            idempotencyKey,
            /* meta */ null);

        try {
            graphService.createTasks(graph);
        } catch (Exception e) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, e, operationDao, responseObserver, LOG))
            {
                return;
            }

            LOG.error("Cannot create tasks for graph: { graph: {}, error: {} }", graph, e.getMessage());
            var errorStatus = Status.INVALID_ARGUMENT.withDescription(e.getMessage());
            responseObserver.onError(errorStatus.asException());
            return;
        }

        responseObserver.onNext(op.toProto());
        responseObserver.onCompleted();
    }

    private static boolean validateRequest(GraphExecuteRequest request,
                                           StreamObserver<LongRunning.Operation> response)
    {
        if (request.getWorkflowId().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("workflowId not set").asException());
            return false;
        }

        if (request.getWorkflowName().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("workflowName not set").asException());
            return false;
        }

        if (request.getUserId().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("userId not set").asException());
            return false;
        }

        if (request.getTasksCount() == 0) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("tasks not set").asException());
            return false;
        }

        return true;
    }
}
