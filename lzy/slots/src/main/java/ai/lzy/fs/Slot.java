package ai.lzy.fs;

import ai.lzy.v1.common.LC;
import ai.lzy.v1.slots.v2.LSA;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface Slot {
    void startTransfer(LC.PeerDescription peer, String transferId);
    void read(long offset, StreamObserver<LSA.ReadDataChunk> transfer);

    String id();
}
