package ai.lzy.longrunning;

import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

public class LocalOperationService extends LongRunningServiceGrpc.LongRunningServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(LocalOperationService.class);

    private final String name;
    private final Map<String, Operation> operations = new ConcurrentHashMap<>();

    public LocalOperationService(String name) {
        this.name = name;
    }

    public boolean registerOperation(Operation operation) {
        LOG.info("[{}] Register operation {}: {}", name, operation.id(), operation.toShortString());

        var existing = operations.putIfAbsent(operation.id(), operation);
        if (existing != null) {
            LOG.error("[{}] Operation {} already exists.", name, operation.id());
            return false;
        }

        return true;
    }

    @Nullable
    public Boolean isDone(String operationId) {
        var op = operations.get(operationId);
        return op != null ? op.done() : null;
    }

    @Override
    public void get(LongRunning.GetOperationRequest request, StreamObserver<LongRunning.Operation> response) {
        LOG.info("OpSrv-{}::get op {}.", name, request.getOperationId());

        var op = operations.get(request.getOperationId());
        if (op == null) {
            var msg = "Operation %s not found".formatted(request.getOperationId());
            LOG.error("OpSrv-%s::get error: %s".formatted(name, msg));
            response.onError(Status.NOT_FOUND.withDescription(msg).asException());
            return;
        }

        LongRunning.Operation protoOp;

        synchronized (op) {
            if (op.done()) {
                if (op.response() != null) {
                    LOG.info("OpSrv-{}::get: operation {} successfully completed.", name, op.id());
                } else if (op.error() != null) {
                    LOG.info("OpSrv-{}::get: operation {} failed with error {}.", name, op.id(), op.error());
                } else {
                    LOG.error("OpSrv-{}::get: operation {} is in unknown completed state {}.",
                        name, op.id(), op.toString());
                }
            } else {
                LOG.info("OpSrv{}::get: operation {} is in progress", name, op.id());
            }
            protoOp = op.toProto();
        }

        response.onNext(protoOp);
        response.onCompleted();
    }
}
