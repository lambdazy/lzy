package ai.lzy.allocator.services;

import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunning.GetOperationRequest;
import ai.lzy.v1.longrunning.LongRunning.Operation;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceImplBase;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class OperationApi extends LongRunningServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(OperationApi.class);

    private final OperationDao operations;

    @Inject
    public OperationApi(OperationDao operations) {
        this.operations = operations;
    }

    @Override
    public void get(GetOperationRequest request, StreamObserver<Operation> responseObserver) {
        ai.lzy.longrunning.Operation op;
        try {
            op = withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> operations.get(request.getOperationId(), null));
        } catch (Exception ex) {
            LOG.error("Cannot get operation {}: {}", request.getOperationId(), ex.getMessage(), ex);
            responseObserver.onError(
                Status.INTERNAL.withDescription("Database error: " + ex.getMessage()).asException());
            return;
        }

        if (op != null) {
            responseObserver.onNext(op.toProto());
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.NOT_FOUND.withDescription("Operation not found").asException());
        }
    }

    @Override
    public void cancel(LongRunning.CancelOperationRequest request, StreamObserver<Operation> responseObserver) {
        // TODO(artolord) add more logic here

        ai.lzy.longrunning.Operation op;
        try {
            op = withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> operations.get(request.getOperationId(), null));
        } catch (Exception ex) {
            LOG.error("Cannot get operation {}: {}", request.getOperationId(), ex.getMessage(), ex);
            responseObserver.onError(
                Status.INTERNAL.withDescription("Database error: " + ex.getMessage()).asException());
            return;
        }

        if (op == null) {
            responseObserver.onError(Status.NOT_FOUND.withDescription("Operation not found").asException());
            return;
        }

        op.setError(Status.CANCELLED);

        try {
            withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> operations.update(op, null));
        } catch (Exception ex) {
            LOG.error("Cannot cancel operation {}: {}", request.getOperationId(), ex.getMessage(), ex);
            responseObserver.onError(
                Status.INTERNAL.withDescription("Database error: " + ex.getMessage()).asException());
            return;
        }

        responseObserver.onNext(op.toProto());
        responseObserver.onCompleted();
    }
}
