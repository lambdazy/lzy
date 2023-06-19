package ai.lzy.fs;

import ai.lzy.v1.common.LC;
import ai.lzy.v1.slots.v2.LSA;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface Slot {
    /**
     * Prepare slot for work with task
     * @return future that completes when slot is ready
     */
    CompletableFuture<Void> beforeExecution() throws IOException;

    /**
     * Complete slot after task is finished
     * @return future that completes when slot is completed
     */
    CompletableFuture<Void> afterExecution();

    /**
     * Fail slot. Slot must clean up all resources and fail all pending operations
     */
    void fail();

    void startTransfer(LC.PeerDescription peer);
    void read(long offset, StreamObserver<LSA.ReadDataChunk> transfer);

    String id();
}
