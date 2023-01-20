package ai.lzy.longrunning;

import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.v1.longrunning.LongRunning.CancelOperationRequest;
import ai.lzy.v1.longrunning.LongRunning.Operation;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;
import static ai.lzy.v1.longrunning.LongRunning.GetOperationRequest;

public final class OperationsService extends LongRunningServiceGrpc.LongRunningServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(OperationsService.class);

    private final OperationDao operations;

    public OperationsService(OperationDao operations) {
        this.operations = operations;
    }

    @Override
    public void get(GetOperationRequest request, StreamObserver<Operation> response) {
        ai.lzy.longrunning.Operation operation;
        try {
            operation = withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> operations.get(request.getOperationId(), null));
        } catch (Exception ex) {
            LOG.error("Cannot get operation {}: {}", request.getOperationId(), ex.getMessage(), ex);
            response.onError(
                Status.INTERNAL.withDescription("Database error: " + ex.getMessage()).asException());
            return;
        }

        if (operation != null) {
            response.onNext(operation.toProto());
            response.onCompleted();
        } else {
            response.onError(Status.NOT_FOUND.withDescription("Operation not found").asException());
        }
    }

    @Override
    public void cancel(CancelOperationRequest request, StreamObserver<Operation> response) {
        LOG.info("Cancel operation {}", request.getOperationId());
        try {
            var status = toProto(Status.CANCELLED.withDescription(request.getMessage()));
            var operation = withRetries(LOG, () -> operations.fail(request.getOperationId(), status, null));
            response.onNext(operation.toProto());
            response.onCompleted();
        } catch (OperationCompletedException e) {
            LOG.error("Cannot cancel operation {}: already completed", request.getOperationId());
            response.onError(Status.INVALID_ARGUMENT.withDescription("Operation already completed").asException());
        } catch (NotFoundException e) {
            LOG.error("Cannot cancel operation {}: not found", request.getOperationId());
            response.onError(Status.NOT_FOUND.withDescription("Operation not found").asException());
        } catch (Exception ex) {
            LOG.error("Cannot cancel operation {}: {}", request.getOperationId(), ex.getMessage(), ex);
            response.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
        }
    }
}
