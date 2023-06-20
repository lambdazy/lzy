package ai.lzy.fs.fs;

import ai.lzy.fs.slots.LzySlotBase;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.slots.LSA;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public abstract class LzyOutputSlotBase extends LzySlotBase implements LzyOutputSlot {
    protected final Logger log = LogManager.getLogger(getClass());
    protected final AtomicInteger completedReads = new AtomicInteger(0);

    protected LzyOutputSlotBase(SlotInstance slotInstance) {
        super(slotInstance);
    }

    @Deprecated
    protected Stream<ByteString> readFromPosition(long offset) throws IOException {
        return Stream.empty();
    }

    @Override
    public void readFromPosition(long offset, StreamObserver<LSA.SlotDataChunk> responseObserver) {
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
                        log.error("Error while writing into streamObserver for slot {}", name(), e);
                        responseObserver.onError(Status.INTERNAL.asException());
                        throw e;
                    }
                }
            );

            responseObserver.onNext(LSA.SlotDataChunk.newBuilder()
                .setControl(LSA.SlotDataChunk.Control.EOS)
                .build());

            completedReads.getAndIncrement();

            responseObserver.onCompleted();
        } catch (IOException e) {
            log.error("Error while reading from slot {}", name(), e);
            responseObserver.onError(Status.INTERNAL.withDescription("Error while reading from slot").asException());
        }
    }

    @Override
    public final int getCompletedReads() {
        return completedReads.get();
    }

}
