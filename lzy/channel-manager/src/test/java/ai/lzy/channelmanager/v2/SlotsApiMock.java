package ai.lzy.channelmanager.v2;

import ai.lzy.v1.slots.v2.LSA.StartTransmissionRequest;
import ai.lzy.v1.slots.v2.LSA.StartTransmissionResponse;
import ai.lzy.v1.slots.v2.LzySlotsApiGrpc;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class SlotsApiMock extends LzySlotsApiGrpc.LzySlotsApiImplBase {
    private final Map<String, CompletableFuture<StartTransmissionRequest>> waiters = new ConcurrentHashMap<>();

    @Override
    public void startTransmission(StartTransmissionRequest request,
                                  StreamObserver<StartTransmissionResponse> responseObserver)
    {
        var future = waiters.get(request.getLoaderPeerId());

        if (future != null) {
            future.complete(request);
        }

        responseObserver.onNext(StartTransmissionResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    public CompletableFuture<StartTransmissionRequest> waitForStartTransmission(String peerId) {
        var future = new CompletableFuture<StartTransmissionRequest>();

        waiters.put(peerId, future);

        return future;
    }
}
