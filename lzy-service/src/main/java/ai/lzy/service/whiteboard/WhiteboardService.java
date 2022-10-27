package ai.lzy.service.whiteboard;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.v1.whiteboard.LWBPS;
import ai.lzy.v1.whiteboard.LzyWhiteboardPrivateServiceGrpc;
import ai.lzy.v1.workflow.LWFS.CreateWhiteboardRequest;
import ai.lzy.v1.workflow.LWFS.CreateWhiteboardResponse;
import ai.lzy.v1.workflow.LWFS.LinkWhiteboardRequest;
import ai.lzy.v1.workflow.LWFS.LinkWhiteboardResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.lzy.service.whiteboard.ProtoConverter.newLWBPSCreateWhiteboardRequest;

public class WhiteboardService {
    private static final Logger LOG = LogManager.getLogger(WhiteboardService.class);

    private final LzyWhiteboardPrivateServiceGrpc.LzyWhiteboardPrivateServiceBlockingStub client;

    public WhiteboardService(LzyWhiteboardPrivateServiceGrpc.LzyWhiteboardPrivateServiceBlockingStub client) {
        this.client = client;
    }

    public void createWhiteboard(CreateWhiteboardRequest request, StreamObserver<CreateWhiteboardResponse> response) {
        var userId = AuthenticationContext.currentSubject().id();

        LWBPS.CreateWhiteboardResponse createdWhiteboard;
        try {
            createdWhiteboard = client.createWhiteboard(newLWBPSCreateWhiteboardRequest(userId, request));
        } catch (StatusRuntimeException e) {
            var causeStatus = e.getStatus();
            LOG.error("Cannot create whiteboard: { userId: {}, whiteboardName: {} }, error: {}",
                userId, request.getName(), causeStatus.getDescription());
            response.onError(causeStatus.withDescription("Cannot create whiteboard: " + causeStatus.getDescription())
                .asRuntimeException());
            return;
        }

        if (!createdWhiteboard.hasWhiteboard()) {
            LOG.error("Cannot create whiteboard: { userId: {}, whiteboardName: {} }, error: {}",
                userId, request.getName(), "empty response from whiteboard private api");
            response.onError(Status.INTERNAL.withDescription("Cannot create whiteboard").asRuntimeException());
            return;
        }

        response.onNext(CreateWhiteboardResponse.newBuilder()
            .setWhiteboardId(createdWhiteboard.getWhiteboard().getId())
            .build());
        response.onCompleted();
    }

    public void linkWhiteboard(LinkWhiteboardRequest request, StreamObserver<LinkWhiteboardResponse> response) {
        response.onError(Status.UNIMPLEMENTED.asRuntimeException());
        response.onCompleted();
    }
}
