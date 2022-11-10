package ai.lzy.longrunning;

import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.v1.longrunning.LongRunning.CancelOperationRequest;
import ai.lzy.v1.longrunning.LongRunning.Operation;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.v1.longrunning.LongRunning.GetOperationRequest;

public class OperationService extends LongRunningServiceGrpc.LongRunningServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(OperationService.class);

    private final OperationDao operations;

    public OperationService(OperationDao operations) {
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
        // TODO(artolord) add more logic here

        ai.lzy.longrunning.Operation operation;

        try {
            var status = com.google.rpc.Status.newBuilder().setCode(Status.CANCELLED.getCode().value());
            operation = withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> operations.updateError(request.getOperationId(), status.build().toByteArray(), null));
        } catch (Exception ex) {
            LOG.error("Cannot cancel operation {}: {}", request.getOperationId(), ex.getMessage(), ex);
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
}
