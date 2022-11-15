package ai.lzy.longrunning;

import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;
import javax.annotation.Nullable;

public enum OperationUtils {
    ;

    public static LongRunning.Operation awaitOperationDone(LongRunningServiceBlockingStub grpcClient,
                                                           String operationId, Duration timeout)
    {
        long nano = timeout.toNanos();
        long deadline = System.nanoTime() + nano;

        LongRunning.Operation result;

        while (true) {
            // TODO: ssokolvyak -- replace on streaming request
            result = grpcClient.get(LongRunning.GetOperationRequest.newBuilder().setOperationId(operationId).build());

            if (result.getDone() || deadline - System.nanoTime() <= 0L) {
                break;
            }

            LockSupport.parkNanos(Duration.ofMillis(300).toNanos());
        }

        return result;
    }

    @Nullable
    public static <T extends Message> T extractResponseOrNull(LongRunning.Operation operation, Class<T> responseType)
        throws InvalidProtocolBufferException
    {
        return operation.getDone() ? operation.getResponse().unpack(responseType) : null;
    }
}
