package ai.lzy.whiteboard.grpc;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.v1.whiteboard.LWBS;
import ai.lzy.v1.whiteboard.LzyWhiteboardServiceGrpc;
import ai.lzy.whiteboard.access.AccessManager;
import ai.lzy.whiteboard.model.Field;
import ai.lzy.whiteboard.model.LinkedField;
import ai.lzy.whiteboard.model.Whiteboard;
import ai.lzy.whiteboard.storage.WhiteboardDataSource;
import ai.lzy.whiteboard.storage.WhiteboardStorage;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.model.grpc.ProtoConverter.fromProto;

public class WhiteboardService extends LzyWhiteboardServiceGrpc.LzyWhiteboardServiceImplBase {

    private static final Logger LOG = LogManager.getLogger(WhiteboardService.class);

    private final AccessManager accessManager;
    private final WhiteboardStorage whiteboardStorage;
    private final WhiteboardDataSource dataSource;

    @Inject
    public WhiteboardService(AccessManager accessManager,
                             WhiteboardStorage whiteboardStorage,
                             WhiteboardDataSource dataSource)
    {
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
                createdAtLowerBound = ai.lzy.util.grpc.ProtoConverter.fromProto(
                    request.getCreatedTimeBounds().getFrom());
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

    @Override
    public void createWhiteboard(LWBS.CreateWhiteboardRequest request,
                                 StreamObserver<LWBS.CreateWhiteboardResponse> responseObserver)
    {
        LOG.info("Create whiteboard {}", request.getWhiteboardName());

        try {
            final var authenticationContext = AuthenticationContext.current();
            final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();

            if (!ProtoValidator.isValid(request)) {
                String errorMessage = "Request shouldn't contain empty fields";
                LOG.error("Create whiteboard failed, invalid argument: {}", errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }

            final String whiteboardId = "whiteboard-" + UUID.randomUUID();
            final Instant createdAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            final Map<String, Field> fields = request.getFieldsList().stream()
                    .map(ProtoConverter::fromProto)
                    .collect(Collectors.toMap(Field::name, x -> x));
            final Whiteboard whiteboard = new Whiteboard(whiteboardId, request.getWhiteboardName(), fields,
                    new HashSet<>(request.getTagsList()), ProtoConverter.fromProto(request.getStorage()),
                    request.getNamespace(), Whiteboard.Status.CREATED, createdAt);

            withRetries(defaultRetryPolicy(), LOG, () ->
                    whiteboardStorage.insertWhiteboard(userId, whiteboard, null));

            try {
                accessManager.addAccess(userId, whiteboardId);
            } catch (Exception e) {
                String errorMessage = "Failed to get access to whiteboard for user " + userId;
                LOG.error("Create whiteboard {} failed, got exception: {}", request.getWhiteboardName(), errorMessage, e);
                LOG.info("Undo creating whiteboard {}, id = {}", request.getWhiteboardName(), whiteboardId);
                withRetries(defaultRetryPolicy(), LOG, () ->
                        whiteboardStorage.deleteWhiteboard(whiteboardId, null));
                LOG.info("Undo creating whiteboard {} done", request.getWhiteboardName());
                responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            }

            responseObserver.onNext(LWBS.CreateWhiteboardResponse.newBuilder()
                    .setWhiteboard(ProtoConverter.toProto(whiteboard))
                    .build());
            LOG.info("Create whiteboard {} done, id = {}", request.getWhiteboardName(), whiteboardId);
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Create whiteboard {} failed, invalid argument: {}", request.getWhiteboardName(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Create whiteboard {} failed, got exception: {}", request.getWhiteboardName(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void linkField(LWBS.LinkFieldRequest request,
                          StreamObserver<LWBS.LinkFieldResponse> responseObserver)
    {
        LOG.info("Finalize field {} of whiteboard {}", request.getFieldName(), request.getWhiteboardId());

        try {
            final var authenticationContext = AuthenticationContext.current();
            final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();

            final String whiteboardId = request.getWhiteboardId();
            final String fieldName = request.getFieldName();

            if (!accessManager.checkAccess(userId, whiteboardId)) {
                LOG.error("PERMISSION DENIED: <{}> trying to access whiteboard <{}> without permission",
                    userId, whiteboardId);
                responseObserver.onError(Status.PERMISSION_DENIED.withDescription(
                    "You don't have access to whiteboard <" + whiteboardId + ">").asException());
                return;
            }

            if (!ProtoValidator.isValid(request)) {
                String errorMessage = "Request shouldn't contain empty fields";
                LOG.error("Finalize field {} of whiteboard {} failed, invalid argument: {}",
                        request.getFieldName(), request.getWhiteboardId(), errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
            }

            final Instant finalizedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            final var linkedField = new LinkedField(fieldName, Field.Status.FINALIZED, request.getStorageUri(),
                    fromProto(request.getScheme()));

            withRetries(defaultRetryPolicy(), LOG, () -> {
                try (final var transaction = TransactionHandle.create(dataSource)) {
                    final Whiteboard whiteboard = whiteboardStorage.getWhiteboard(whiteboardId, transaction);

                    final Field field = whiteboard.getField(fieldName);
                    if (field == null) {
                        throw new NotFoundException("Field " + fieldName + " of whiteboard " + whiteboardId + " not found");
                    }
                    if (field.status() == Field.Status.FINALIZED) {
                        throw new IllegalArgumentException("Field " + fieldName + " of whiteboard " + whiteboardId + " already finalized");
                    }
                    if (field instanceof LinkedField oldLinkedField) {
                        LOG.info("Field {} of whiteboard {} has already linked with data [{}]. " +
                                "Data will be overwritten.", fieldName, whiteboardId, oldLinkedField.toString());
                    }

                    whiteboardStorage.updateField(whiteboardId, linkedField, finalizedAt, transaction);

                    transaction.commit();
                }
            });

            responseObserver.onNext(LWBS.LinkFieldResponse.getDefaultInstance());
            LOG.info("Finalize field {} of whiteboard {} done", fieldName, whiteboardId);
            responseObserver.onCompleted();
        } catch (NotFoundException e) {
            LOG.error("Finalize field {} of whiteboard {} failed, not found exception: {}",
                    request.getFieldName(), request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
        } catch (IllegalArgumentException e) {
            LOG.error("Finalize field {} of whiteboard {} failed, invalid argument: {}",
                    request.getFieldName(), request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Finalize field {} of whiteboard {} failed, got exception: {}",
                    request.getFieldName(), request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void finalizeWhiteboard(LWBS.FinalizeWhiteboardRequest request,
                                   StreamObserver<LWBS.FinalizeWhiteboardResponse> responseObserver)
    {
        LOG.info("Finalize whiteboard {}", request.getWhiteboardId());

        try {

            final var authenticationContext = AuthenticationContext.current();
            final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();

            final String whiteboardId = request.getWhiteboardId();
            if (whiteboardId.isBlank()) {
                throw new IllegalArgumentException("Request shouldn't contain empty fields");
            }

            if (!accessManager.checkAccess(userId, whiteboardId)) {
                LOG.error("PERMISSION DENIED: <{}> trying to access whiteboard <{}> without permission",
                        userId, whiteboardId);
                responseObserver.onError(Status.PERMISSION_DENIED.withDescription(
                        "You don't have access to whiteboard <" + whiteboardId + ">").asException());
                return;
            }

            final Instant finalizedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

            withRetries(defaultRetryPolicy(), LOG, () -> {
                try (final var transaction = TransactionHandle.create(dataSource)) {
                    final Whiteboard whiteboard = whiteboardStorage.getWhiteboard(whiteboardId, transaction);

                    if (whiteboard.status() == Whiteboard.Status.FINALIZED) {
                        throw new IllegalArgumentException("Whiteboard " + whiteboardId + " already finalized");
                    }

                    final var unlinkedFields = whiteboard.unlinkedFields();
                    if (!unlinkedFields.isEmpty()) {
                        LOG.info("Finalize whiteboard {} with unlinked fields: [{}]",
                                whiteboardId, String.join(",", unlinkedFields.toString()));
                    }

                    whiteboardStorage.finalizeWhiteboard(whiteboardId, finalizedAt, transaction);

                    transaction.commit();
                }
            });

            responseObserver.onNext(LWBS.FinalizeWhiteboardResponse.getDefaultInstance());
            LOG.info("Finalize whiteboard {} done", whiteboardId);
            responseObserver.onCompleted();
        } catch (NotFoundException e) {
            LOG.error("Finalize whiteboard {} failed, not found exception: {}",
                    request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
        } catch (IllegalArgumentException e) {
            LOG.error("Finalize whiteboard {} failed, invalid argument: {}",
                    request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Finalize whiteboard {} failed, got exception: {}",
                    request.getWhiteboardId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

}
