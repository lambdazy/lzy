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

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

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
        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> operations.get(request.getOperationId(), null),
            op -> {
                if (op != null) {
                    responseObserver.onNext(op.toProto());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(Status.NOT_FOUND.withDescription("Operation not found").asException());
                }
            },
            ex -> {
                LOG.error("Cannot get operation {}: {}", request.getOperationId(), ex.getMessage(), ex);
                responseObserver.onError(
                    Status.INTERNAL.withDescription("Database error: " + ex.getMessage()).asException());
            });
    }

    @Override
    public void cancel(OperationService.CancelOperationRequest request, StreamObserver<Operation> responseObserver) {
        // TODO(artolord) add more logic here

        final ai.lzy.allocator.model.Operation[] opRef = {null};
        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> operations.get(request.getOperationId(), null),
            op -> {
                if (op != null) {
                    opRef[0] = op;
                } else {
                    responseObserver.onError(Status.NOT_FOUND.withDescription("Operation not found").asException());
                }
            },
            ex -> {
                LOG.error("Cannot get operation {}: {}", request.getOperationId(), ex.getMessage(), ex);
                responseObserver.onError(
                    Status.INTERNAL.withDescription("Database error: " + ex.getMessage()).asException());
            }
        );

        final var op = opRef[0];
        if (op == null) {
            return;
        }

        op.complete(Status.CANCELLED);

        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                operations.update(op, null);
                return (Void) null;
            },
            ok -> {
                responseObserver.onNext(op.toProto());
                responseObserver.onCompleted();
            },
            ex -> {
                LOG.error("Cannot cancel operation {}: {}", request.getOperationId(), ex.getMessage(), ex);
                responseObserver.onError(
                    Status.INTERNAL.withDescription("Database error: " + ex.getMessage()).asException());
            }
        );
    }
}
