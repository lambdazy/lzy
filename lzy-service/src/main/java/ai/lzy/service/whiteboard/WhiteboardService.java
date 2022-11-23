package ai.lzy.service.whiteboard;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.whiteboard.LWBPS;
import ai.lzy.v1.whiteboard.LzyWhiteboardPrivateServiceGrpc;
import ai.lzy.v1.workflow.LWFS.CreateWhiteboardRequest;
import ai.lzy.v1.workflow.LWFS.CreateWhiteboardResponse;
import ai.lzy.v1.workflow.LWFS.LinkWhiteboardRequest;
import ai.lzy.v1.workflow.LWFS.LinkWhiteboardResponse;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.lzy.service.LzyService.APP;
import static ai.lzy.service.whiteboard.ProtoConverter.newLWBPSCreateWhiteboardRequest;
import static ai.lzy.service.whiteboard.ProtoConverter.newLWBPSLinkFieldRequest;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;

@Singleton
public class WhiteboardService {
    private static final Logger LOG = LogManager.getLogger(WhiteboardService.class);

    private final LzyWhiteboardPrivateServiceGrpc.LzyWhiteboardPrivateServiceBlockingStub client;

    public WhiteboardService(@Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
                             @Named("WhiteboardServiceChannel") ManagedChannel whiteboardChannel)
    {
        this.client = newBlockingClient(
            LzyWhiteboardPrivateServiceGrpc.newBlockingStub(whiteboardChannel), APP,
            () -> internalUserCredentials.get().token());
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
        try {
            //noinspection ResultOfMethodCallIgnored
            client.linkField(newLWBPSLinkFieldRequest(request));
        } catch (StatusRuntimeException e) {
            var status = e.getStatus();
            LOG.error("Cannot link whiteboard field: { whiteboardId: {}, fieldName: {}, storageUri: {} }, error: {}",
                request.getWhiteboardId(), request.getFieldName(), request.getStorageUri(), status.getDescription());
            response.onError(status.withDescription("Cannot link whiteboard field: " + status.getDescription())
                .asRuntimeException());
            return;
        }

        response.onNext(LinkWhiteboardResponse.getDefaultInstance());
        response.onCompleted();
    }
}
