package ai.lzy.slots;

import ai.lzy.v1.slots.LSA;
import ai.lzy.v1.slots.LSA.StartTransferRequest;
import ai.lzy.v1.slots.LSA.StartTransferResponse;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import com.google.protobuf.TextFormat;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SlotsService extends LzySlotsApiGrpc.LzySlotsApiImplBase {
    private static final Logger LOG = LogManager.getLogger(SlotsService.class);

    private final Map<String, Slot> slots = new ConcurrentHashMap<>();

    public void register(Slot slot) {
        slots.put(slot.id(), slot);
    }

    public void unregister(String slotId) {
        slots.remove(slotId);
    }

    @Override
    public void read(LSA.ReadDataRequest request, StreamObserver<LSA.ReadDataChunk> responseObserver) {
        LOG.info("Read request: {}", TextFormat.printer().shortDebugString(request));

        var slot = slots.get(request.getPeerId());
        if (slot == null) {
            LOG.error("Slot not found: {}", request.getPeerId());
            responseObserver.onError(Status.NOT_FOUND.asException());
            return;
        }

        try {
            slot.read(request.getOffset(), responseObserver);
        } catch (Exception e) {
            LOG.error("Failed to read from slot", e);
            responseObserver.onError(Status.INTERNAL.asException());
        }
    }

    @Override
    public void startTransfer(StartTransferRequest request, StreamObserver<StartTransferResponse> responseObserver) {
        LOG.info("Start transfer request: {}", TextFormat.printer().shortDebugString(request));

        var slot = slots.get(request.getSlotId());
        if (slot == null) {
            LOG.error("Slot not found: {}", request.getSlotId());
            responseObserver.onError(Status.NOT_FOUND.asException());
            return;
        }

        try {
            slot.startTransfer(request.getPeer(), request.getTransferId());
        } catch (Exception e) {
            LOG.error("Failed to start transfer", e);
            responseObserver.onError(Status.INTERNAL.asException());
            return;
        }

        responseObserver.onNext(StartTransferResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
