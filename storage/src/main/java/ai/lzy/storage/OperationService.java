package ai.lzy.storage;

import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class OperationService extends LongRunningServiceGrpc.LongRunningServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(OperationService.class);

    private final WithInMemoryOperationStorage service;

    public OperationService(WithInMemoryOperationStorage service) {
        this.service = service;
    }

    @Override
    public void get(LongRunning.GetOperationRequest request, StreamObserver<LongRunning.Operation> response) {
        LOG.info("YandexCloudS3Storage::OperationApi::get op {}.", request.getOperationId());

        var operation = service.getOperations().values().stream()
            .filter(op -> op.id().contentEquals(request.getOperationId())).findFirst().orElse(null);

        if (operation == null) {
            var msg = "Operation %s not found".formatted(request.getOperationId());
            LOG.error(msg);
            response.onError(Status.NOT_FOUND.withDescription(msg).asException());
            return;
        }

        synchronized (operation) {
            if (operation.done()) {
                if (operation.response() != null) {
                    LOG.info("Operation {} successfully completed.", operation.id());
                } else if (operation.error() != null) {
                    LOG.info("Operation {} failed with error {}.", operation.id(), operation.error());
                } else {
                    LOG.error("Operation {} is in unknown completed state {}.", operation.id(), operation.toString());
                }
                service.getOperations().remove(operation.id());
            }
        }

        response.onNext(operation.toProto());
        response.onCompleted();
    }
}
