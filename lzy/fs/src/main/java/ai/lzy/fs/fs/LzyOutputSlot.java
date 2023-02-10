package ai.lzy.fs.fs;

import ai.lzy.v1.slots.LSA;
import io.grpc.stub.StreamObserver;

public interface LzyOutputSlot extends LzySlot {
    /**
     * Blocking function to read data from slot into responseObserver
     *
     * @param offset           offset of data, only for not streaming slots
     * @param responseObserver observer to read data into
     */
    void readFromPosition(long offset, StreamObserver<LSA.SlotDataChunk> responseObserver);

    int getCompletedReads();
}
