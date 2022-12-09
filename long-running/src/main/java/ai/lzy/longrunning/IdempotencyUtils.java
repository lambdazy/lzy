package ai.lzy.longrunning;

import ai.lzy.longrunning.LocalOperationService.ImmutableCopyOperation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.BytesValue;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.logging.log4j.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.concurrent.locks.LockSupport;
import javax.annotation.Nullable;

import static ai.lzy.longrunning.dao.OperationDao.OPERATION_IDEMPOTENCY_KEY_CONSTRAINT;
import static ai.lzy.model.db.DbHelper.isUniqueViolation;
import static ai.lzy.model.db.DbHelper.withRetries;

public final class IdempotencyUtils {
    public static <T extends Message> void replyWaitedOpResult(LocalOperationService opService, String opId,
                                                               StreamObserver<T> responseObserver,
                                                               Class<T> responseType,
                                                               String internalErrorMessage, Logger log)
    {
        replyWaitedOpResult(opService, opId, responseObserver, responseType, false, internalErrorMessage, log);
    }

    public static <T extends Message> void replyWaitedOpResult(LocalOperationService operationService, String opId,
                                                               StreamObserver<T> responseObserver,
                                                               Class<T> responseType,
                                                               boolean isListType,
                                                               String internalErrorMessage, Logger log)
    {
        var opSnapshot = operationService.awaitOperationCompletion(opId, Duration.ofMillis(50), Duration.ofSeconds(5));

        if (opSnapshot == null) {
            log.error("Can not find operation with id: { opId: {} }", opId);
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
            return;
        }

        if (opSnapshot.done()) {
            if (opSnapshot.response() != null) {
                try {
                    if (isListType) {
                        var bytesMessage = opSnapshot.response().unpack(BytesValue.class);
                        var bytes = bytesMessage.toByteString().toByteArray();
                        ArrayList<T> list = SerializationUtils.deserialize(bytes);

                        list.forEach(responseObserver::onNext);
                        responseObserver.onCompleted();
                    } else {
                        var resp = opSnapshot.response().unpack(responseType);
                        responseObserver.onNext(resp);
                        responseObserver.onCompleted();
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.error("Error while waiting op result: {}", e.getMessage(), e);
                    responseObserver.onError(Status.INTERNAL.asRuntimeException());
                }
            } else {
                var error = opSnapshot.error();
                assert error != null;
                responseObserver.onError(error.asRuntimeException());
            }
        } else {
            log.error("Waiting deadline exceeded, operation: {}", opSnapshot.toShortString());
            responseObserver.onError(Status.INTERNAL.withDescription(internalErrorMessage).asRuntimeException());
        }
    }

    public static <T extends Message> boolean loadExistingOpResult(OperationDao operationsDao,
                                                                   Operation.IdempotencyKey idempotencyKey,
                                                                   StreamObserver<T> response, Class<T> responseType,
                                                                   Duration loadAttemptDelay, Duration timeout,
                                                                   Logger log)
    {
        long deadline = System.nanoTime() + timeout.toNanos();
        Operation op;

        try {
            op = withRetries(log, () -> operationsDao.getByIdempotencyKey(idempotencyKey.token(), null));
            if (op == null) {
                return false; // key doesn't exist
            }

            if (!idempotencyKey.equals(op.idempotencyKey())) {
                log.error("Idempotency key {} conflict", idempotencyKey.token());
                response.onError(Status.INVALID_ARGUMENT
                    .withDescription("IdempotencyKey conflict").asException());
                return true;
            }
        } catch (Exception ex) {
            log.error("Error while loading operation by idempotency key {}: {}",
                idempotencyKey.token(), ex.getMessage(), ex);
            response.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return true;
        }

        var opId = op.id();

        log.info("Found existing op {} with idempotency key {}", opId, idempotencyKey.token());

        while (!op.done() && deadline - System.nanoTime() > 0L) {
            LockSupport.parkNanos(loadAttemptDelay.toNanos());

            try {
                op = withRetries(log, () -> operationsDao.get(opId, null));
            } catch (Exception e) {
                log.error("Error while loading operation by idempotency key {}: {}",
                    idempotencyKey.token(), e.getMessage(), e);
                response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
                return true;
            }
        }

        if (!op.done()) {
            log.error("Error while loading operation by idempotency key {}: {}",
                idempotencyKey.token(), "Cannot await completion of concurrent call which processed operation");
            response.onError(Status.INTERNAL.withDescription("Concurrent call error").asException());
            return true;
        }

        var resp = op.response();

        if (resp != null) {
            try {
                response.onNext(resp.unpack(responseType));
            } catch (InvalidProtocolBufferException e) {
                log.error("Error while loading operation by idempotency key {}: {}",
                    idempotencyKey.token(), e.getMessage(), e);
                response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
                return true;
            }
            response.onCompleted();
        } else {
            var error = op.error();
            assert error != null;
            response.onError(error.asRuntimeException());
        }

        return true;
    }

    public static <T extends Message> boolean loadExistingOpResult(LocalOperationService opService,
                                                                   Operation.IdempotencyKey idempotencyKey,
                                                                   Class<T> responseType,
                                                                   StreamObserver<T> response,
                                                                   String internalErrorMessage,
                                                                   Logger log)
    {
        return loadExistingOpResult(opService, idempotencyKey, false, responseType, response, internalErrorMessage,
            log);
    }

    public static <T extends Message> boolean loadExistingOpResult(LocalOperationService opService,
                                                                   Operation.IdempotencyKey idempotencyKey,
                                                                   boolean isListType,
                                                                   Class<T> responseType,
                                                                   StreamObserver<T> response,
                                                                   String internalErrorMessage,
                                                                   Logger log)
    {
        ImmutableCopyOperation opSnapshot = opService.getByIdempotencyKey(idempotencyKey.token());

        if (opSnapshot != null) {
            if (!idempotencyKey.equals(opSnapshot.idempotencyKey())) {
                log.error("Idempotency key {} conflict", idempotencyKey.token());
                response.onError(Status.INVALID_ARGUMENT.withDescription("IdempotencyKey conflict").asException());
                return true;
            }

            log.info("Found operation by idempotency key: {}", opSnapshot.toString());

            var opId = opSnapshot.id();
            opSnapshot = opService.awaitOperationCompletion(opId, Duration.ofMillis(50), Duration.ofSeconds(5));

            if (opSnapshot == null) {
                log.warn("Operation unexpectedly dissapeared from storage: { opId: {} }", opId);
                return false;
            }

            if (opSnapshot.done()) {
                if (opSnapshot.response() != null) {
                    try {
                        if (isListType) {
                            var bytesMessage = opSnapshot.response().unpack(BytesValue.class);
                            var bytes = bytesMessage.toByteString().toByteArray();
                            ArrayList<T> list = SerializationUtils.deserialize(bytes);

                            list.forEach(response::onNext);
                        } else {
                            var resp = opSnapshot.response().unpack(responseType);
                            response.onNext(resp);
                        }

                        response.onCompleted();
                    } catch (InvalidProtocolBufferException e) {
                        log.error("Cannot serialize result of operation: { opId: {} }, error: {}", opId,
                            e.getMessage(), e);
                        response.onError(Status.INTERNAL.withDescription(internalErrorMessage).asRuntimeException());
                    }
                } else {
                    var error = opSnapshot.error();
                    assert error != null;
                    response.onError(error.asRuntimeException());
                }
            } else {
                log.error("Waiting deadline exceeded, operation: {}", opSnapshot.toShortString());
                response.onError(Status.INTERNAL.withDescription(internalErrorMessage).asRuntimeException());
            }

            return true;
        } else {
            log.debug("Operation with idempotency key not found: { idempotencyKey: {} }", idempotencyKey.token());
            return false;
        }
    }

    public static boolean loadExistingOp(OperationDao operationsDao, Operation.IdempotencyKey idempotencyKey,
                                         StreamObserver<LongRunning.Operation> response, Logger log)
    {
        try {
            var op = withRetries(log, () -> operationsDao.getByIdempotencyKey(idempotencyKey.token(), null));
            if (op != null) {
                if (!idempotencyKey.equals(op.idempotencyKey())) {
                    log.error("Idempotency key {} conflict", idempotencyKey.token());
                    response.onError(Status.INVALID_ARGUMENT
                        .withDescription("IdempotencyKey conflict").asException());
                    return true;
                }

                log.info("Found existing op {} with idempotency key {}", op.id(), idempotencyKey.token());

                response.onNext(op.toProto());
                response.onCompleted();
                return true;
            }

            return false; // key doesn't exist
        } catch (Exception ex) {
            log.error("Error while loading operation by idempotency key {}: {}",
                idempotencyKey.token(), ex.getMessage(), ex);
            response.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return true;
        }
    }

    public static boolean loadExistingOp(LocalOperationService opService, Operation.IdempotencyKey idempotencyKey,
                                         StreamObserver<LongRunning.Operation> response, Logger log)
    {
        ImmutableCopyOperation opSnapshot = opService.getByIdempotencyKey(idempotencyKey.token());

        if (opSnapshot != null) {
            if (!idempotencyKey.equals(opSnapshot.idempotencyKey())) {
                log.error("Idempotency key {} conflict", idempotencyKey.token());
                response.onError(Status.INVALID_ARGUMENT.withDescription("IdempotencyKey conflict").asException());
                return true;
            }

            log.info("Found operation by idempotency key: {}", opSnapshot.toString());

            response.onNext(opSnapshot.toProto());
            response.onCompleted();
            return true;
        } else {
            log.debug("Operation with idempotency key not found: { idempotencyKey: {} }", idempotencyKey.token());
            return false;
        }
    }

    public static <T extends Message> boolean handleIdempotencyKeyConflict(Operation.IdempotencyKey idempotencyKey,
                                                                           Exception ex, OperationDao operationsDao,
                                                                           StreamObserver<T> response,
                                                                           Class<T> responseType,
                                                                           Duration loadAttemptDelay, Duration timeout,
                                                                           Logger log)
    {
        if (isUniqueViolation(ex, OPERATION_IDEMPOTENCY_KEY_CONSTRAINT)) {
            if (loadExistingOpResult(operationsDao, idempotencyKey, response,
                responseType, loadAttemptDelay, timeout, log))
            {
                return true;
            }

            log.error("Idempotency key {} not found", idempotencyKey.token());
            response.onError(Status.INTERNAL.withDescription("Idempotency key conflict").asException());
            return true;
        }
        return false;
    }

    public static boolean handleIdempotencyKeyConflict(Operation.IdempotencyKey idempotencyKey, Exception ex,
                                                       OperationDao operationsDao,
                                                       StreamObserver<LongRunning.Operation> response, Logger log)
    {
        if (isUniqueViolation(ex, OPERATION_IDEMPOTENCY_KEY_CONSTRAINT)) {
            if (loadExistingOp(operationsDao, idempotencyKey, response, log)) {
                return true;
            }

            log.error("Idempotency key {} not found", idempotencyKey.token());
            response.onError(Status.INTERNAL.withDescription("Idempotency key conflict").asException());
            return true;
        }
        return false;
    }

    @Nullable
    public static Operation.IdempotencyKey getIdempotencyKey(Message request) {
        var idempotencyToken = GrpcHeaders.getIdempotencyKey();
        if (idempotencyToken != null) {
            return new Operation.IdempotencyKey(idempotencyToken, md5(request));
        }
        return null;
    }

    public static String md5(Message message) {
        try {
            var md5 = MessageDigest.getInstance("MD5");
            var bytes = md5.digest(message.toByteArray());
            return HexFormat.of().formatHex(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
