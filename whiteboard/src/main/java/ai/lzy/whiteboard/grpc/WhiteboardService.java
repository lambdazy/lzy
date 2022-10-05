package ai.lzy.whiteboard.grpc;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.v1.whiteboard.LWBS;
import ai.lzy.v1.whiteboard.LzyWhiteboardServiceGrpc;
import ai.lzy.whiteboard.access.AccessManager;
import ai.lzy.whiteboard.model.Whiteboard;
import ai.lzy.whiteboard.storage.WhiteboardDataSource;
import ai.lzy.whiteboard.storage.WhiteboardStorage;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class WhiteboardService extends LzyWhiteboardServiceGrpc.LzyWhiteboardServiceImplBase {

    private static final Logger LOG = LogManager.getLogger(WhiteboardService.class);

    private final AccessManager accessManager;
    private final WhiteboardStorage whiteboardStorage;
    private final WhiteboardDataSource dataSource;

    @Inject
    public WhiteboardService(AccessManager accessManager,
                             WhiteboardStorage whiteboardStorage,
                             WhiteboardDataSource dataSource) {
        this.accessManager = accessManager;
        this.whiteboardStorage = whiteboardStorage;
        this.dataSource = dataSource;
    }

    @Override
    public void get(LWBS.GetRequest request, StreamObserver<LWBS.GetResponse> responseObserver) {
        LOG.info("Get whiteboard {}", request.getWhiteboardId());

        try {
            final var authenticationContext = AuthenticationContext.current();
            final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();
            final String whiteboardId = request.getWhiteboardId();

            if (whiteboardId.isBlank()) {
                final String errorMessage = "Request shouldn't contain empty fields";
                LOG.error("Get whiteboard {} failed, invalid argument: {}", request.getWhiteboardId(), errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
            }

            if (!accessManager.checkAccess(userId, whiteboardId)) {
                LOG.error("Get whiteboard {} failed, permission denied.", request.getWhiteboardId());
                final String clientErrorMessage = "Whiteboard " + whiteboardId + " not found";
                responseObserver.onError(Status.NOT_FOUND.withDescription(clientErrorMessage).asException());
            }

            final Whiteboard whiteboard = whiteboardStorage.getWhiteboard(whiteboardId, null);

            responseObserver.onNext(LWBS.GetResponse.newBuilder()
                .setWhiteboard(ProtoConverter.toProto(whiteboard))
                .build());
            LOG.info("Get whiteboard {} done", whiteboardId);
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Get whiteboard {} failed, invalid argument: {}",
                request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (NotFoundException e) {
            LOG.error("Get whiteboard {} failed, not found exception: {}",
                request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Get whiteboard {} failed, got exception: {}",
                request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void list(LWBS.ListRequest request, StreamObserver<LWBS.ListResponse> responseObserver) {
        LOG.info("List whiteboards");

        try {
            final var authenticationContext = AuthenticationContext.current();
            final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();
            @Nullable String name = request.getName().isBlank() ? null : request.getName();
            List<String> tags = request.getTagsList();
            @Nullable Instant createdAtLowerBound = null;
            if (request.hasCreatedTimeBounds() && request.getCreatedTimeBounds().hasFrom()) {
                createdAtLowerBound = ai.lzy.util.grpc.ProtoConverter.fromProto(request.getCreatedTimeBounds().getFrom());
            }
            @Nullable Instant createdAtUpperBound = null;
            if (request.hasCreatedTimeBounds() && request.getCreatedTimeBounds().hasTo()) {
                createdAtUpperBound = ai.lzy.util.grpc.ProtoConverter.fromProto(request.getCreatedTimeBounds().getTo());
            }

            Stream<Whiteboard> whiteboards = whiteboardStorage.listWhiteboards(userId, name, tags,
                createdAtLowerBound, createdAtUpperBound, null);

            var response = LWBS.ListResponse.newBuilder()
                .addAllWhiteboards(whiteboards.map(ProtoConverter::toProto).toList())
                .build();
            responseObserver.onNext(response);
            LOG.info("List whiteboards done, {} found", response.getWhiteboardsCount());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("List whiteboards failed, invalid argument: {}", e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("List whiteboards failed, got exception: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

}
