package ai.lzy.allocator.services;

import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.v1.OperationService;
import ai.lzy.v1.OperationService.GetOperationRequest;
import ai.lzy.v1.OperationService.Operation;
import ai.lzy.v1.OperationServiceApiGrpc.OperationServiceApiImplBase;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;

@Singleton
public class OperationApi extends OperationServiceApiImplBase {
    private static final Logger LOG = LogManager.getLogger(OperationApi.class);

    private final OperationDao operations;

    @Inject
    public OperationApi(OperationDao operations) {
        this.operations = operations;
    }

    @Override
    public void get(GetOperationRequest request, StreamObserver<Operation> responseObserver) {
        ai.lzy.allocator.model.Operation op;

        try {
            op = operations.get(request.getOperationId(), null);
        } catch (SQLException e) {
            LOG.error("Cannot get operation {}: {}", request.getOperationId(), e.getMessage(), e);
            responseObserver.onError(
                Status.INTERNAL.withDescription("Database error: " + e.getMessage()).asException());
            return;
        }

        if (op == null) {
            responseObserver.onError(Status.NOT_FOUND.withDescription("Operation not found").asException());
            return;
        }

        responseObserver.onNext(op.toProto());
        responseObserver.onCompleted();
    }

    @Override
    public void cancel(OperationService.CancelOperationRequest request, StreamObserver<Operation> responseObserver) {
        // TODO(artolord) add more logic here

        ai.lzy.allocator.model.Operation op;
        try {
            op = operations.get(request.getOperationId(), null);
        } catch (SQLException e) {
            LOG.error("Cannot get operation {}: {}", request.getOperationId(), e.getMessage(), e);
            responseObserver.onError(
                Status.INTERNAL.withDescription("Database error: " + e.getMessage()).asException());
            return;
        }

        if (op == null) {
            responseObserver.onError(Status.NOT_FOUND.withDescription("Operation not found").asException());
            return;
        }

        var newOp = op.complete(Status.CANCELLED);
        try {
            operations.update(newOp, null);
        } catch (SQLException e) {
            LOG.error("Cannot cancel operation {}: {}", request.getOperationId(), e.getMessage(), e);
            responseObserver.onError(
                Status.INTERNAL.withDescription("Database error: " + e.getMessage()).asException());
            return;
        }

        responseObserver.onNext(newOp.toProto());
        responseObserver.onCompleted();
    }
}
