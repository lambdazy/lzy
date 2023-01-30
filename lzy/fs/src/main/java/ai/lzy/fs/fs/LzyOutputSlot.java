package ai.lzy.fs.fs;

import ai.lzy.v1.slots.LSA;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.stream.Stream;

public interface LzyOutputSlot extends LzySlot {
    Logger LOG = LogManager.getLogger(LzyOutputSlot.class);

    /**
     * Deprecated to replace with {@code readFromPosition(long, StreamObserver<LSA.SlotDataChunk>)}
     */
    @Deprecated
    default Stream<ByteString> readFromPosition(long offset) throws IOException {
        return Stream.empty();
    }

    /**
     * Blocking function to read data from slot into responseObserver
     * @param offset offset of data, only for not streaming slots
     * @param responseObserver observer to read data into
     */
    default void readFromPosition(long offset, StreamObserver<LSA.SlotDataChunk> responseObserver) {
        try {
            var stream = readFromPosition(offset);
            stream.forEach(
                (chunk) -> {
                    try {
                        responseObserver.onNext(
                            LSA.SlotDataChunk.newBuilder()
                                .setChunk(chunk)
                                .build()
                        );
                    } catch (StatusRuntimeException e) {
                        LOG.error("Error while writing into streamObserver for slot {}", name(), e);
                        responseObserver.onError(Status.INTERNAL.asException());
                        throw e;
                    }
                }
            );

            responseObserver.onNext(LSA.SlotDataChunk.newBuilder()
                .setControl(LSA.SlotDataChunk.Control.EOS)
                .build());

            responseObserver.onCompleted();
        } catch (IOException e) {
            LOG.error("Error while reading from slot {}", name(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Error while reading from slot").asException());
        }
    }

}
