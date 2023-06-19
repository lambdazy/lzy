package ai.lzy.fs;

import ai.lzy.v1.channel.v2.LCMS.*;
import ai.lzy.v1.channel.v2.LzyChannelManagerGrpc;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class ChannelManagerMock extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {
    private final ConcurrentHashMap<String, RequestHandle<BindResponse, BindRequest>> onBind = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RequestHandle<UnbindResponse, UnbindRequest>> onUnbind = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RequestHandle<TransferCompletedResponse, TransferCompletedRequest>> onTransferCompleted = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RequestHandle<TransferFailedResponse, TransferFailedRequest>> onTransferFailed = new ConcurrentHashMap<>();

    @Override
    public void bind(BindRequest request, StreamObserver<BindResponse> responseObserver) {
        var handle = onBind.get(request.getPeerId());
        if (handle == null) {
            responseObserver.onError(new RuntimeException("Unexpected bind request: " + request));
            return;
        }
        handle.onRequest(request, responseObserver);
    }

    @Override
    public void unbind(UnbindRequest request, StreamObserver<UnbindResponse> responseObserver) {
        var handle = onUnbind.get(request.getPeerId());
        if (handle == null) {
            responseObserver.onError(new RuntimeException("Unexpected unbind request: " + request));
            return;
        }
        handle.onRequest(request, responseObserver);
    }

    @Override
    public void transferCompleted(
        TransferCompletedRequest request, StreamObserver<TransferCompletedResponse> responseObserver)
    {
        var handle = onTransferCompleted.get(request.getPeerId());
        if (handle == null) {
            responseObserver.onError(new RuntimeException("Unexpected transfer completed request: " + request));
            return;
        }
        handle.onRequest(request, responseObserver);
    }

    @Override
    public void transferFailed(TransferFailedRequest request, StreamObserver<TransferFailedResponse> responseObserver) {
        var handle = onTransferFailed.get(request.getPeerId());
        if (handle == null) {
            responseObserver.onError(new RuntimeException("Unexpected transfer failed request: " + request));
            return;
        }
        handle.onRequest(request, responseObserver);
    }

    public RequestHandle<BindRequest, BindResponse> onBind(String peerId) {
        var pair = RequestHandle.<BindRequest, BindResponse>create();
        onBind.put(peerId, pair.getRight());
        return pair.getLeft();
    }

    public RequestHandle<UnbindRequest, UnbindResponse> onUnbind(String peerId) {
        var pair = RequestHandle.<UnbindRequest, UnbindResponse>create();
        onUnbind.put(peerId, pair.getRight());
        return pair.getLeft();
    }

    public RequestHandle<TransferCompletedRequest, TransferCompletedResponse> onTransferCompleted(String peerId) {
        var pair = RequestHandle.<TransferCompletedRequest, TransferCompletedResponse>create();
        onTransferCompleted.put(peerId, pair.getRight());
        return pair.getLeft();
    }

    public RequestHandle<TransferFailedRequest, TransferFailedResponse> onTransferFailed(String peerId) {
        var pair = RequestHandle.<TransferFailedRequest, TransferFailedResponse>create();
        onTransferFailed.put(peerId, pair.getRight());
        return pair.getLeft();
    }

    public static class RequestHandle<Req, Resp> {
        private final CompletableFuture<Req> requestFuture;
        private final CompletableFuture<Resp> responseFuture;

        public RequestHandle(CompletableFuture<Req> requestFuture, CompletableFuture<Resp> responseFuture) {
            this.requestFuture = requestFuture;
            this.responseFuture = responseFuture;
        }

        public Req get() {
            return requestFuture.join();
        }

        public void complete(Resp response) {
            responseFuture.complete(response);
        }

        public void completeExceptionally(Throwable t) {
            responseFuture.completeExceptionally(t);
        }

        private void onRequest(Resp req, StreamObserver<Req> responseObserver) {
            complete(req);
            try {
                var resp = get();
                responseObserver.onNext(resp);
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        private static <Req, Resp> Pair<RequestHandle<Req, Resp>, RequestHandle<Resp, Req>> create() {
            var requestFuture = new CompletableFuture<Req>();
            var responseFuture = new CompletableFuture<Resp>();
            return Pair.of(new RequestHandle<>(requestFuture, responseFuture),
                new RequestHandle<> (responseFuture, requestFuture));
        }
    }
}
