package ai.lzy.longrunning;

import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

public enum OperationGrpcServiceUtils {
    ;

    public static LongRunning.Operation awaitOperationDone(LongRunningServiceBlockingStub grpcClient,
                                                           String operationId, Duration timeout)
    {
        long nano = timeout.toNanos();
        long deadline = System.nanoTime() + nano;

        LongRunning.Operation result;

        while (true) {
            result = grpcClient.get(LongRunning.GetOperationRequest.newBuilder().setOperationId(operationId).build());

            if (result.getDone() || deadline - System.nanoTime() <= 0L) {
                break;
            }

            LockSupport.parkNanos(Duration.ofMillis(300).toNanos());
        }

        return result;
    }
}
