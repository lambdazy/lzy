package ai.lzy.graph;

import ai.lzy.graph.config.ServiceConfig;
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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOp;
import static ai.lzy.util.grpc.ProtoPrinter.safePrinter;

@Singleton
public class GraphExecutorApi extends GraphExecutorGrpc.GraphExecutorImplBase {
    private static final Logger LOG = LogManager.getLogger(GraphExecutorApi.class);

    private final GraphService graphService;
    private final OperationDao operationDao;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @Inject
    public GraphExecutorApi(ServiceConfig config, GraphService graphService,
                            @Named("GraphExecutorOperationDao") OperationDao operationDao)
    {
        this.graphService = graphService;
        this.operationDao = operationDao;

        /*
        executor.schedule(() -> {
            try {
                operationDao.deleteOutdatedOperations(config.getGcPeriod().toHoursPart());
            } catch (SQLException e) {
                LOG.error("Cannot delete outdated operations");
            }
        }, config.getGcPeriod().getSeconds(), TimeUnit.SECONDS);
        */
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }

    @Override
    public void execute(LGE.ExecuteGraphRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
        if (!validateRequest(request, responseObserver)) {
            return;
        }

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationDao, idempotencyKey, responseObserver, LOG)) {
            return;
        }

        var op = Operation.create(
            request.getUserId(),
            "Execute graph: executionId='%s'".formatted(request.getExecutionId()),
            null,
            idempotencyKey,
            /* meta */ null);

        try {
            graphService.runGraph(request, op);
        } catch (Exception e) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, e, operationDao, responseObserver, LOG))
            {
                return;
            }

            LOG.error("Cannot execute graph, request: {}, error: {}",
                safePrinter().shortDebugString(request), e.getMessage());
            var errorStatus = Status.INVALID_ARGUMENT.withDescription(e.getMessage());
            responseObserver.onError(errorStatus.asException());
            return;
        }

        responseObserver.onNext(op.toProto());
        responseObserver.onCompleted();
    }

    private static boolean validateRequest(LGE.ExecuteGraphRequest request,
                                           StreamObserver<LongRunning.Operation> response)
    {
        if (request.getUserId().isBlank()) {
            return invalidArgument(response, "user_id");
        }

        if (request.getWorkflowName().isBlank()) {
            return invalidArgument(response, "workflow_name");
        }

        if (request.getExecutionId().isBlank()) {
            return invalidArgument(response, "execution_id");
        }

        if (request.getAllocatorSessionId().isBlank()) {
            return invalidArgument(response, "allocator_session_id");
        }

        if (request.getTasksCount() == 0) {
            return invalidArgument(response, "tasks");
        }

        return true;
    }

    private static boolean invalidArgument(StreamObserver<LongRunning.Operation> response, String arg) {
        response.onError(Status.INVALID_ARGUMENT.withDescription(arg + " not set").asException());
        return false;
    }
}
