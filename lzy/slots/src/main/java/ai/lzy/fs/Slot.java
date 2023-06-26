package ai.lzy.fs;

import ai.lzy.v1.common.LC;
import ai.lzy.v1.slots.v2.LSA;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.NotImplementedException;

interface Slot {
    /**
     * Start transfer from the given peer
     * Throws NotImplementedException if the slot is not capable of starting transfer
     * Throws IllegalStateException if the slot is already in use
     */
    void startTransfer(LC.PeerDescription peer, String transferId)
        throws NotImplementedException, IllegalStateException;

    void read(long offset, StreamObserver<LSA.ReadDataChunk> transfer);

    String id();
}
