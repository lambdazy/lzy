package ai.lzy.portal;

import ai.lzy.v1.whiteboard.LWBPS;
import ai.lzy.v1.whiteboard.LzyWhiteboardPrivateServiceGrpc;
import io.grpc.stub.StreamObserver;

public class WhiteboardPrivateApiMock extends LzyWhiteboardPrivateServiceGrpc.LzyWhiteboardPrivateServiceImplBase {
    @Override
    public void createWhiteboard(LWBPS.CreateWhiteboardRequest request,
                                 StreamObserver<LWBPS.CreateWhiteboardResponse> responseObserver)
    {
        responseObserver.onNext(LWBPS.CreateWhiteboardResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void linkField(LWBPS.LinkFieldRequest request, StreamObserver<LWBPS.LinkFieldResponse> responseObserver) {
        responseObserver.onNext(LWBPS.LinkFieldResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void finalizeWhiteboard(LWBPS.FinalizeWhiteboardRequest request,
                                   StreamObserver<LWBPS.FinalizeWhiteboardResponse> responseObserver)
    {
        responseObserver.onNext(LWBPS.FinalizeWhiteboardResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
