package ai.lzy.channelmanager.v2;

import ai.lzy.v1.slots.v2.LSA.StartTransferRequest;
import ai.lzy.v1.slots.v2.LSA.StartTransferResponse;
import ai.lzy.v1.slots.v2.LzySlotsApiGrpc;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class SlotsApiMock extends LzySlotsApiGrpc.LzySlotsApiImplBase {
    private final Map<String, CompletableFuture<StartTransferRequest>> waiters = new ConcurrentHashMap<>();

    @Override
    public void startTransfer(StartTransferRequest request,
                              StreamObserver<StartTransferResponse> responseObserver)
    {
        var future = waiters.get(request.getSlotId());

        if (future != null) {
            future.complete(request);
        }

        responseObserver.onNext(StartTransferResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    public CompletableFuture<StartTransferRequest> waitForStartTransfer(String peerId) {
        var future = new CompletableFuture<StartTransferRequest>();

        waiters.put(peerId, future);

        return future;
    }
}
