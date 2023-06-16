package ai.lzy.longrunning;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

public enum LocalOperationServiceUtils {
    ;

    public static <T extends Message> void awaitOpAndReply(LocalOperationService operationService, String opId,
                                                           StreamObserver<T> responseObserver,
                                                           Class<T> responseType,
                                                           String internalErrorMessage, Logger log)
    {
        if (!operationService.await(opId, Duration.ofSeconds(5))) {
            log.error("Cannot await operation completion: { opId: {} }", opId);
            responseObserver.onError(Status.INTERNAL.withDescription(internalErrorMessage).asRuntimeException());
            return;
        }

        var opSnapshot = operationService.get(opId);

        assert opSnapshot != null;

        if (opSnapshot.response() != null) {
            try {
                var resp = opSnapshot.response().unpack(responseType);
                responseObserver.onNext(resp);
                responseObserver.onCompleted();
            } catch (InvalidProtocolBufferException e) {
                log.error("Cannot parse operation result: { opId: {}, error: {} }", e.getMessage(), e);
                responseObserver.onError(Status.INTERNAL.withDescription(internalErrorMessage).asRuntimeException());
            }
        } else {
            var error = opSnapshot.error();
            assert error != null;
            responseObserver.onError(error.asRuntimeException());
        }
    }
}
